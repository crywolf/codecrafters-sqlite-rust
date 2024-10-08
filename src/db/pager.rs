use super::{
    db_info::DBInfo,
    page::{Cell, Page, TableLeafCell},
};

use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
};

use anyhow::{Context, Result};

#[derive(Debug)]
pub(super) struct Pager<T>
where
    T: Read + Seek,
{
    file: T,
    page_size: u16,
    use_cache: bool,
    page_cache: HashMap<u64, Page>,
}

impl<T> Pager<T>
where
    T: Read + Seek,
{
    pub(super) fn new(file: T) -> Result<(Self, DBInfo)> {
        let (db_info, file) = DBInfo::new(file)?;
        Ok((
            Self {
                file,
                page_size: db_info.page_size,
                use_cache: false,
                page_cache: HashMap::new(),
            },
            db_info,
        ))
    }

    pub(super) fn get_page(&mut self, num: u64) -> Result<Page> {
        if !self.use_cache {
            return self.load_page(num);
        }

        // use cache
        if let Some(page) = self.page_cache.get(&num) {
            Ok(page.clone())
        } else {
            let page = self.load_page(num)?;
            self.page_cache.insert(num, page.clone());
            Ok(page)
        }
    }

    fn load_page(&mut self, num: u64) -> Result<Page> {
        let page_start = (num - 1) * self.page_size as u64;

        self.file
            .seek(SeekFrom::Start(page_start))
            .context("seek offset in the DB file")?;

        Page::load(&mut self.file, self.page_size).context("loading page from file")
    }
}

pub(super) struct Tree<'a, T>
where
    T: Read + Seek,
{
    pager: &'a mut Pager<T>,
    table_root_page: u64,
    cells: Vec<TableLeafCell>,
}

impl<'a, T> Tree<'a, T>
where
    T: Read + Seek,
{
    pub(super) fn new(pager: &'a mut Pager<T>) -> Self {
        Self {
            pager,
            table_root_page: 0,
            cells: Vec::new(),
        }
    }

    pub(super) fn cells(
        &mut self,
        page: u64,
        filter: Option<CellFilter>,
    ) -> Result<&Vec<TableLeafCell>> {
        self.table_root_page = page;
        let mut root_page = page;

        if let Some(f) = &filter {
            if let Some(index_root_page) = f.index_root_page {
                self.pager.use_cache = true; // we want to use page cache when searching with index
                root_page = index_root_page;
            }
        }

        self.load_page_cells(root_page, &filter)
            .with_context(|| format!("load tree cells for root page {page}"))?;
        Ok(&self.cells)
    }

    fn load_page_cells(&mut self, page: u64, filter: &Option<CellFilter>) -> Result<()> {
        let page = self
            .pager
            .get_page(page)
            .with_context(|| format!("get page {page}"))?;

        for (i, cell) in page.cells.into_iter().enumerate() {
            match cell {
                Cell::TableLeaf(table_leaf_cell) => {
                    if let Some(f) = filter {
                        let record_val = table_leaf_cell.column(f.col_i, f.primary_key_col_i)?;
                        if f.val != record_val.to_string().to_lowercase() {
                            continue;
                        }
                    }
                    self.cells.push(table_leaf_cell)
                }
                Cell::TableInterior(table_interior_cell) => {
                    if let Some(f) = filter {
                        if f.col_i == f.primary_key_col_i {
                            // filtering by row_id
                            let searched_row_id = f
                                .val
                                .parse()
                                .context("row_id value in filter is not number")?;

                            if table_interior_cell.row_id() >= searched_row_id {
                                // we either found searched key or are behind it => read from child page
                                let page_num = table_interior_cell.left_child_page();
                                self.load_page_cells(page_num as u64, filter).with_context(
                                    || format!("load cells for table child page {page_num}"),
                                )?;
                            }

                            if table_interior_cell.row_id() > searched_row_id {
                                break;
                            }
                            // if we read the last cell on the page,
                            // move on to the page on the right side of the tree if some
                            if i == page.n_cells as usize - 1 {
                                if let Some(page_num) = page.rightmost_pointer {
                                    self.load_page_cells(page_num as u64, filter).with_context(
                                        || {
                                            format!(
                                        "load cells for right-most table interior page {page_num}"
                                    )
                                        },
                                    )?;
                                }
                            }
                            continue;
                        }
                    }

                    // Full table scan

                    let page_num = table_interior_cell.left_child_page();
                    self.load_page_cells(page_num as u64, filter)
                        .with_context(|| format!("load cells for table child page {page_num}"))?;

                    // if we read the last cell on the page,
                    // move on to the page on the right side of the tree if some
                    if i == page.n_cells as usize - 1 {
                        if let Some(page_num) = page.rightmost_pointer {
                            self.load_page_cells(page_num as u64, filter)
                                .with_context(|| {
                                    format!(
                                        "load cells for right-most table interior page {page_num}"
                                    )
                                })?;
                        }
                    }
                }
                Cell::IndexLeaf(index_leaf_cell) => {
                    if let Some(f) = filter {
                        if f.val == index_leaf_cell.key()?.to_string() {
                            // find the page with the row_id and get the cell
                            let table_root_page_num = self.table_root_page;
                            let mut row_id_filter = f.clone();
                            row_id_filter.col_i = row_id_filter.primary_key_col_i; // serarching by row_id (primary key)
                            row_id_filter.val = index_leaf_cell.row_id()?.to_string();

                            self.load_page_cells(
                                table_root_page_num,
                                &Some(row_id_filter),
                            )
                            .with_context(|| {
                                format!("load cells for table root page filtered by row_id {table_root_page_num}")
                            })?;
                        }
                    }
                }
                Cell::IndexInterior(index_interior_cell) => {
                    if let Some(f) = filter {
                        if index_interior_cell.key()?.to_string() >= f.val {
                            if index_interior_cell.key()?.to_string() == f.val {
                                // find the page with the row_id and get the cell
                                let table_root_page_num = self.table_root_page;
                                let mut row_id_filter = f.clone();
                                row_id_filter.col_i = row_id_filter.primary_key_col_i; // serarching by row_id (primary key)
                                row_id_filter.val = index_interior_cell.row_id()?.to_string();

                                self.load_page_cells(
                                    table_root_page_num,
                                    &Some(row_id_filter),
                                )
                                .with_context(|| {
                                    format!("load cells for table root page filtered by row_id {table_root_page_num}")
                                })?;
                            }

                            // we either found searched key or are behind it => read from child page
                            let page_num = index_interior_cell.left_child_page();
                            self.load_page_cells(page_num as u64, filter)
                                .with_context(|| {
                                    format!("load cells for index child page {page_num}")
                                })?;

                            if index_interior_cell.key()?.to_string() > f.val {
                                break;
                            }
                            // if we read last cell on the page and didn't find anything greater than searched value,
                            // move on to the page on the right side of the tree if some
                            if i == page.n_cells as usize - 1 {
                                if let Some(page_num) = page.rightmost_pointer {
                                    self.load_page_cells(page_num as u64, filter).with_context(
                                        || {
                                            format!(
                                        "load cells for right-most index interior page {page_num}"
                                    )
                                        },
                                    )?;
                                }
                            }
                        } else {
                            // skipping everything lesser the the searched value
                            continue;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct CellFilter {
    index_root_page: Option<u64>,
    col_i: u16,
    val: String,
    primary_key_col_i: u16,
}

impl CellFilter {
    pub(super) fn new(
        index_root_page: Option<u64>,
        col_i: u16,
        val: String,
        primary_key_col_i: u16,
    ) -> Self {
        Self {
            index_root_page,
            col_i,
            val,
            primary_key_col_i,
        }
    }
}
