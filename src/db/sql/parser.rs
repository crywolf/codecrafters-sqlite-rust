use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1, space0, space1},
    combinator::opt,
    multi::{many1, separated_list0},
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum ParsedCommand {
    Select,
    Create(u16), // parameter is primary key column index
}

#[derive(Debug, PartialEq)]
pub struct Parsed {
    pub command: ParsedCommand,
    pub columns: Vec<String>,
    pub table: String,
}

pub fn parse_select(sql: &str) -> IResult<&str, Parsed> {
    let (rem, _) = tag_no_case("SELECT")(sql)?;
    let (rem, _) = space1(rem)?;

    let (rem, count) = opt(preceded(
        tag_no_case("COUNT"),
        alt((
            delimited(tag("("), parse_field, tag(")")),
            delimited(tag("("), tag("*"), tag(")")),
        )),
    ))(rem)?;

    let col_str;

    let (rem, columns) = if let Some(column) = count {
        col_str = format!("count({})", column);
        (rem, vec![col_str.as_str()])
    } else {
        separated_list0(tuple((space0, tag(","), space0)), parse_field)(rem)?
    };

    let columns = columns.into_iter().map(|c| c.to_lowercase()).collect();

    let (rem, _) = space1(rem)?;
    let (rem, _) = tag_no_case("FROM")(rem)?;
    let (rem, _) = space1(rem)?;
    let (rem, table) = terminated(parse_field, space0)(rem)?;

    Ok((
        rem,
        Parsed {
            command: ParsedCommand::Select,
            columns,
            table: table.to_lowercase(),
        },
    ))
}

pub fn parse_create_table(sql: &str) -> IResult<&str, Parsed> {
    /*
    CREATE TABLE apples
    (
        id integer primary key autoincrement,
        name text,
        color text
    )
    */
    let (rem, _) = multispace0(sql)?;
    let (rem, _) = tag_no_case("CREATE")(rem)?;
    let (rem, _) = space1(rem)?;
    let (rem, _) = tag_no_case("TABLE")(rem)?;
    let (rem, _) = space1(rem)?;
    let (rem, table) = parse_field(rem)?;
    let (rem, _) = multispace1(rem)?;
    let (rem, columns) = parse_columns(rem)?;

    let mut primary_key = 0;
    let columns = columns
        .into_iter()
        .enumerate()
        .map(|(i, (col, is_primary))| {
            if is_primary {
                primary_key = i as u16;
            }
            col.to_string()
        })
        .collect();

    Ok((
        rem,
        Parsed {
            command: ParsedCommand::Create(primary_key),
            columns,
            table: table.to_string(),
        },
    ))
}

fn parse_field(input: &str) -> IResult<&str, &str> {
    let (rem, v) = alt((
        delimited(
            // field in quotes can contain space or _
            char('"'),
            take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == ' '),
            char('"'),
        ),
        // regular field
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)?;

    Ok((rem, v))
}

fn parse_column_name(input: &str) -> IResult<&str, (&str, bool)> {
    let (rem, v) = terminated(
        many1(tuple((multispace0, parse_field, multispace0))),
        alt((tag(","), multispace0)),
    )(input)?;

    let is_primary_key = v.iter().any(|t| t.1 == "primary");

    Ok((rem, (v[0].1, is_primary_key)))
}

fn all_columns(input: &str) -> IResult<&str, Vec<(&str, bool)>> {
    let (rem, v) = many1(parse_column_name)(input)?;
    Ok((rem, v))
}

fn parse_columns(input: &str) -> IResult<&str, Vec<(&str, bool)>> {
    let (rem, _) = multispace0(input)?;
    let (rem, v) = delimited(tag("("), all_columns, tag(")"))(rem)?;
    let (rem, _) = multispace0(rem)?;

    Ok((rem, v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field() {
        let sql = "name ,";
        let o = parse_field(sql);
        let o = o.unwrap();
        assert_eq!(o, (" ,", "name"));

        let sql = "year_founded2, another";
        let o = parse_field(sql);
        let o = o.unwrap();
        assert_eq!(o, (", another", "year_founded2"));

        let sql = "\"size range\" text";
        let o = parse_field(sql);
        let o = o.unwrap();
        assert_eq!(o, (" text", "size range"));
    }

    #[test]
    fn test_parse_sql_count_asterix_uppercase() {
        let sql = "SELECT COUNT(*) FROM oranges     ";
        let c = parse_select(sql);
        let c = c.unwrap();
        assert_eq!(
            c.1,
            Parsed {
                command: ParsedCommand::Select,
                columns: vec!["count(*)".to_string()],
                table: "oranges".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_sql_count_column_uppercase() {
        let sql = "SELECT COUNT(NAME) FROM ORANGES";
        let c = parse_select(sql);
        let c = c.unwrap();
        assert_eq!(
            c.1,
            Parsed {
                command: ParsedCommand::Select,
                columns: vec!["count(name)".to_string()],
                table: "oranges".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_sql_select_column_uppercase() {
        let sql = "SELECT NAME FROM ORANGES";
        let c = parse_select(sql);
        let c = c.unwrap();
        assert_eq!(
            c.1,
            Parsed {
                command: ParsedCommand::Select,
                columns: vec!["name".to_string()],
                table: "oranges".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_sql_select_multiple_columns_uppercase() {
        let sql = "SELECT id, name ,descr FROM oranges";
        let c = parse_select(sql);
        let c = c.unwrap();
        assert_eq!(
            c.1,
            Parsed {
                command: ParsedCommand::Select,
                columns: vec!["id".to_string(), "name".to_string(), "descr".to_string()],
                table: "oranges".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_column_name() {
        let sql = "\n	id integer primary key autoincrement, \n";
        let c = parse_column_name(sql);
        let (_, column) = c.unwrap();
        assert_eq!(column, ("id", true));

        let sql = "\n	color text \n";
        let c = parse_column_name(sql);
        let (_, column) = c.unwrap();
        assert_eq!(column, ("color", false));
    }

    #[test]
    fn test_all_columns() {
        // multiline
        let sql = "\n id integer primary key autoincrement,
	name text,
	color text\n";
        let c = all_columns(sql);
        let (_, columns) = c.unwrap();
        assert_eq!(
            columns,
            vec![("id", true), ("name", false), ("color", false)]
        );

        // one line
        let sql = "id integer primary key autoincrement,name text,color text";
        let c = all_columns(sql);
        let (_, columns) = c.unwrap();
        assert_eq!(
            columns,
            vec![("id", true), ("name", false), ("color", false)]
        );
    }

    #[test]
    fn test_parse_columns() {
        let sql = "(\n	id integer primary key autoincrement,
	name text,
	color text\n)";
        let c = parse_columns(sql);
        let (_, columns) = c.unwrap();
        assert_eq!(
            columns,
            vec![("id", true), ("name", false), ("color", false)]
        );
    }

    #[test]
    fn test_parse_sql_create_table() {
        let sql = "\n CREATE TABLE companies2
(
	id integer primary key autoincrement,
	name text,
    year_produced text ,
   \"size range\" text,
	color text
)
";
        let c = parse_create_table(sql);
        let c = c.unwrap();
        assert_eq!(
            c.1,
            Parsed {
                command: ParsedCommand::Create(0),
                columns: vec![
                    "id".to_string(),
                    "name".to_string(),
                    "year_produced".to_string(),
                    "size range".to_string(),
                    "color".to_string()
                ],
                table: "companies2".to_string(),
            },
        );
    }
}
