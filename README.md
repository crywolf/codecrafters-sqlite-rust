[![progress-banner](https://backend.codecrafters.io/progress/sqlite/2af6b53d-0eb9-46c5-9382-65a25402cb36)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).

In this challenge, you'll build a barebones SQLite implementation that supports
basic SQL queries like `SELECT`. Along the way we'll learn about
[SQLite's file format](https://www.sqlite.org/fileformat.html), how indexed data
is
[stored in B-trees](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

Some links I found useful:
* https://medium.com/basecs/busying-oneself-with-b-trees-78bbf10522e7
* https://fly.io/blog/sqlite-internals-btree/
* https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/
* https://blog.sylver.dev/build-your-own-sqlite-part-1-listing-tables

# Sample Databases

To make it easy to test queries locally, we've added a sample database in the
root of this repository: `sample.db`.

This contains two tables: `apples` & `oranges`. You can use this to test your
implementation for the first 6 stages.

You can explore this database by running queries against it like this:

```sh
$ sqlite3 sample.db "select id, name from apples"
1|Granny Smith
2|Fuji
3|Honeycrisp
4|Golden Delicious
```

There are two other databases that you can use:

1. `superheroes.db`:
   - This is a small version of the test database used in the table-scan stage.
   - It contains one table: `superheroes`.
   - It is ~1MB in size.
1. `companies.db`:
   - This is a small version of the test database used in the index-scan stage.
   - It contains one table: `companies`, and one index: `idx_companies_country`
   - It is ~7MB in size.

These aren't included in the repository because they're large in size. You can
download them by running this script:

```sh
./download_sample_databases.sh
```

If the script doesn't work for some reason, you can download the databases
directly from
[codecrafters-io/sample-sqlite-databases](https://github.com/codecrafters-io/sample-sqlite-databases).
