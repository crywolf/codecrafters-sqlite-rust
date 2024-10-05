use super::{
    db_info::DBInfo,
    page::{Cell, Page, TableLeafCell},
};

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result};

#[derive(Debug)]
pub(super) struct Pager<T>
where
    T: Read + Seek,
{
    file: T,
    page_size: u16,
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
            },
            db_info,
        ))
    }

    pub(super) fn get_page(&mut self, num: u64) -> Result<Page> {
        let page_start = (num - 1) * self.page_size as u64;

        self.file
            .seek(SeekFrom::Start(page_start))
            .context("seek offset in the DB file")?;

        Page::load(&mut self.file, self.page_size)
    }
}

pub(super) struct Tree<'a, T>
where
    T: Read + Seek,
{
    pager: &'a mut Pager<T>,
    cells: Vec<TableLeafCell>,
}

impl<'a, T> Tree<'a, T>
where
    T: Read + Seek,
{
    pub(super) fn new(pager: &'a mut Pager<T>) -> Self {
        Self {
            pager,
            cells: Vec::new(),
        }
    }

    pub(super) fn cells(&mut self, page: u64) -> Result<&Vec<TableLeafCell>> {
        self.load_page_cells(page)
            .with_context(|| format!("load tree cells for root page {page}"))?;
        Ok(&self.cells)
    }

    fn load_page_cells(&mut self, page: u64) -> Result<()> {
        let page = self
            .pager
            .get_page(page)
            .with_context(|| format!("get page {page}"))?;

        for (i, cell) in page.cells.into_iter().enumerate() {
            match cell {
                Cell::TableLeaf(table_leaf_cell) => self.cells.push(table_leaf_cell),
                Cell::TableInterior(table_interior_cell) => {
                    let page_num = table_interior_cell.left_child_page;
                    self.load_page_cells(page_num as u64)
                        .with_context(|| format!("load cells for child page {page_num}"))?;

                    // if we read last cell on the page,
                    // move on to the the page on the right side of the tree if some
                    if i == page.n_cells as usize - 1 {
                        if let Some(page_num) = page.rightmost_pointer {
                            self.load_page_cells(page_num as u64).with_context(|| {
                                format!("load cells for right-most page {page_num}")
                            })?;
                        }
                    }
                }
                Cell::IndexLeaf(_index_leaf_cell) => {
                    eprintln!("Skipping Index Leaf Cell for now",);
                    continue;
                }
            }
        }

        Ok(())
    }
}
