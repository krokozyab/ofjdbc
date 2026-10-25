# DUCK Workspace — Reference

The `DUCK` schema is an embedded [DuckDB](https://duckdb.org) database that lives next to Fusion. This page is the
detailed reference for everything the driver does with it: how statements are routed, how imports are remembered and
refreshed, what `DECODE` adds to an import, how snapshots, `DIFF` and `HISTORY` work, how local keys are sent into a
Fusion filter, how `JOIN KEYS` finds paths, and what happens to your files when the DuckDB version changes.

Every command here is plain SQL typed in the editor of your client (DBeaver, DataGrip, …). Answers come back as ordinary
result sets, so the client can sort, filter and export them. Nothing needs an extra tool.

## Contents

1. [Basics: engines, the workspace file, hidden tables](#1-basics)
2. [Transactions](#2-transactions)
3. [Mixed queries and the next result page](#3-mixed-queries-and-the-next-result-page)
4. [Filter Fusion by a list kept in DUCK](#4-filter-fusion-by-a-list-kept-in-duck)
5. [Export to Parquet or CSV](#5-export-to-parquet-or-csv)
6. [Datasets: the registry and `DUCK TABLES`](#6-datasets-the-registry-and-duck-tables)
7. [`REFRESH`: full and incremental](#7-refresh-full-and-incremental)
8. [Readable imports: `DECODE`](#8-readable-imports-decode)
9. [Snapshots and `DIFF`](#9-snapshots-and-diff)
10. [`HISTORY`: results you can look at again](#10-history-results-you-can-look-at-again)
11. [`JOIN KEYS` and join paths](#11-join-keys-and-join-paths)
12. [DuckDB 2.0 and your existing files](#12-duckdb-20-and-your-existing-files)
13. [Command summary](#13-command-summary)
14. [Messages and what to do](#14-messages-and-what-to-do)

---

## 1. Basics

**Which engine runs a statement** is always explicit:

| The statement… | Runs in |
|---|---|
| has no `DUCK.` reference (unqualified names included, whatever schema the client selected) | Oracle Fusion |
| references `DUCK.x` | DuckDB |
| starts with `#d` or `#duck` | DuckDB, as written — its whole dialect, `read_csv`, `COPY`, `SUMMARIZE`, `PRAGMA`, … |
| is `CREATE [OR REPLACE] TABLE DUCK.x AS <query>` or `INSERT INTO DUCK.x <query>` where the query reads Fusion | an **import**: Fusion is read, DuckDB stores the rows |
| reads both `DUCK.` and `FUSION.` | a **mixed query** (section 3) |
| is one of the driver commands in this page (`DUCK TABLES`, `REFRESH`, `HISTORY`, `SNAPSHOT`, `DIFF`, …) | the driver, which uses both |

**The file.** `~/.ofjdbc/duck.duckdb`, shared by all your Fusion connections, so data imported from DEV, TEST and PROD
can be compared in one query. Use `?duckDb=<file>` (or the property `ofjdbc.duckDb`, `-Dofjdbc.duckDb`,
`OFJDBC_DUCK_DB`) for another file, or `:memory:` for none — then nothing is written to disk.

**Hidden tables.** The driver keeps its own bookkeeping in tables whose names start with `__ofjdbc_` — the registry, the
snapshots, the history, work tables of mixed queries. They are not shown in the tree and not returned by the JDBC
metadata, but you can query them with `#d` when you want to look inside.

**Imports.** Rows stream page by page (5000 per request) from Fusion into DuckDB. Column types are inferred from all the
values (`BIGINT`, `DECIMAL`, `DATE`, `TIMESTAMP`, else text) and refined by Fusion's own dictionary: columns that are text
there (segments, `ATTRIBUTEn`, codes such as `0010`) stay text; `NUMBER(18)` ids become `BIGINT`; amounts and rates
without a declared precision become `DECIMAL(38,10)` so rows appended later are not rounded; an Oracle `DATE` becomes
`TIMESTAMP` (it carries a time of day, even when every value read is at midnight) and `TIMESTAMP WITH TIME ZONE`
becomes `TIMESTAMPTZ`. The dictionary type of a column is used only when the result column is that column — written
as is, through its table alias, renamed with `AS`, or under `*` — and never for an expression that merely carries its
name (`TO_CHAR(creation_date, 'YYYY') creation_date` stays a number). If DuckDB's parser cannot read the Oracle query
(`(+)` joins, …) only "text" is taken from the dictionary, by name. A Fusion query that returns
no rows carries no column names. When the driver can read the columns from your `SELECT` list (with the dictionary for
their types) it still creates an empty table; otherwise (for example `SELECT *` of a table it does not know) `CREATE TABLE … AS`
fails with an explanation — create the table first and use `INSERT INTO … SELECT`. An empty `INSERT` simply adds nothing.

## 2. Transactions

By default DUCK statements **commit at once**, whatever mode your SQL client asks for. DBeaver's Smart commit switches a
connection to manual mode by itself after a `CREATE` or `INSERT`; with local transactions an uncommitted table would be
invisible to the navigator's connection (dropping it from the tree fails) and one failed statement would block every
later one until a Rollback. When a client turns auto-commit off, the driver says once that DUCK stays in auto-commit;
Commit and Rollback do nothing for DUCK tables. `REFRESH` and imports still make their own transaction, so a failed load
leaves the old data.

With **`duckTransactions=true`** (URL `?duckTransactions=true`, connection property `ofjdbc.duckTransactions`,
`OFJDBC_DUCK_TRANSACTIONS`) Auto-commit, Commit and Rollback in your SQL client apply to the DUCK tables. Fusion has no
transactions, so only the local workspace is affected: switch auto-commit off, change a DUCK table, press Rollback, and
the change is undone. A connection closed with an unfinished local transaction discards it. The mode you choose before
the first DUCK statement is applied when the workspace session opens. `REFRESH` (section 7) then runs inside your
transaction and leaves committing to you.

In that mode a DUCK statement that fails (a typo, a missing table) aborts the transaction: DuckDB then refuses every
statement — imports included — until it ends. The driver says so and names the error that started it; press Rollback
(or switch to Auto-commit) and run the statement again. DBeaver also opens a separate connection per SQL editor: a table
created there stays invisible to the navigator's connection until that editor commits.

## 3. Mixed queries and the next result page

A query can read both engines. `UNION`/`INTERSECT`/`MINUS`/`EXCEPT` branches and parenthesized subqueries that read
**only** `FUSION.*` are fetched from Fusion first into hidden work tables; then the whole statement runs in DuckDB
(`MINUS` is run as `EXCEPT`):

```sql
SELECT l.*, f.enabled_flag
  FROM DUCK.gcc l
  JOIN (SELECT code_combination_id, enabled_flag FROM FUSION.GL_CODE_COMBINATIONS
         WHERE segment2 = '1110') f ON f.code_combination_id = l.code_combination_id;
```

Keep the Fusion part filtered — it is fetched in full. A Fusion table joined directly (`JOIN FUSION.x`) is rejected with
a hint to use a parenthesized subquery or to import the table.

**The next result page.** DBeaver does not keep the cursor open between pages: for page two it closes the statement
and runs the same SQL again with a larger fetch size. For a mixed query that would mean fetching the Fusion parts again
on every page. The driver keeps the fetched parts: a re-run of the same statement with a **larger** fetch size within
the cache time (`nextPageCacheSeconds`, default 300 s) reuses them (a warning says so). Refresh (same fetch size), a
different statement or an expired entry fetches afresh. The work tables are dropped when the connection closes. If a
Rollback removed them, they are fetched again.

## 4. Filter Fusion by a list kept in DUCK

The consultant's typical job: a list of 200 invoice numbers in a spreadsheet, and a table in Fusion with millions of
rows. Put the list in DUCK (`#d CREATE TABLE DUCK.ids AS SELECT * FROM read_csv('ids.csv')`) and use it in the
Fusion filter:

```sql
SELECT f.* FROM (
  SELECT invoice_id, invoice_num, invoice_amount FROM FUSION.AP_INVOICES_ALL
   WHERE invoice_num IN (SELECT invoice_num FROM DUCK.ids)
) f;
```

**What happens.** The driver runs `SELECT invoice_num FROM DUCK.ids` in DuckDB, takes the distinct non-null values and
writes them into the Fusion query as a literal list. Fusion then reads only those rows:

```sql
… WHERE (invoice_num IN ('INV-001', 'INV-002', …))
```

- **Value rendering.** Numbers as numbers, text in quotes (an apostrophe is doubled), dates as `TO_DATE(…)`, timestamps
  as `TO_TIMESTAMP(…)`.
- **Long lists.** Oracle accepts 1000 items per `IN`; a longer list becomes
  `(col IN (…1000…) OR col IN (…1000…))`. No values at all becomes `1=0` (an empty `IN` is not valid), so the Fusion
  query returns nothing.
- **Limits.** At most 50,000 distinct values, and the whole Fusion query must fit 32,767 bytes (the report receives SQL
  as a PL/SQL `VARCHAR2`; about 200 bytes are kept for the paging clause). Beyond that the driver stops **before**
  asking Fusion and tells you; the way out is to import the Fusion rows and join them locally.
- **Rules.** `column IN (SELECT one_expression FROM DUCK.t [WHERE …])`: one plain column on the left (alias-qualified is
  fine); the subquery may use functions, nested `IN` lists, parentheses and literals with parentheses — its end is found
  by matching parentheses. It runs in DuckDB on its own, so it must select one column, read only DUCK tables (and its
  own CTEs) and not refer to the outer query; otherwise the driver says which rule it breaks before Fusion is asked. A
  subquery reading FUSION as well is not a key list. `NOT IN` is **not** supported — its handling of `NULL` changes
  once the values are copied. Several such filters in one part are fine.
- **Cache.** The literal values are part of the key of the reused-parts cache (section 3): changing `DUCK.ids` never
  returns stale Fusion rows.
- **Warning.** `pushed 5 distinct key value(s) from DUCK into the Fusion filter on invoice_id`.

On a live Fusion dev, five ids went to Fusion as `invoice_id IN (163063, 169020, 170019, 181025, 181026)` and five rows
came back.

## 5. Export to Parquet or CSV

A `#d` statement can read Fusion in a parenthesized subquery and write straight to a file, with no table in the
workspace:

```sql
#d COPY (SELECT * FROM FUSION.GL_JE_LINES WHERE period_name = 'Oct-26') TO 'gl_oct.parquet' (FORMAT PARQUET);
#d COPY (SELECT * FROM FUSION.AP_INVOICES_ALL WHERE invoice_date > DATE '2026-01-01') TO 'ap.csv' (FORMAT CSV, HEADER);
#d CREATE TABLE DUCK.ids AS SELECT * FROM read_csv('ids.csv');
```

Combine with section 4: `COPY (SELECT … FROM FUSION.x WHERE col IN (SELECT col FROM DUCK.ids)) TO …`. A relative path
is relative to the process that runs the client; use an absolute one when in doubt. Read the file back with
`#d SELECT * FROM read_parquet('gl_oct.parquet')`.

## 6. Datasets: the registry and `DUCK TABLES`

Every import is written to a **registry** inside the workspace (the hidden table `__ofjdbc_datasets`):

| Field | Meaning |
|---|---|
| table | the DUCK table |
| Fusion host, user | which environment and which login it was read with |
| source SQL | the query, without paging, used again by `REFRESH` |
| time, row count | of the last import or refresh |
| key columns, watermark column and value | set by an incremental `REFRESH` (section 7) |
| decode clause | the `DECODE …` text, applied again by `REFRESH` (section 8) |
| source type | `FUSION` (a Fusion query), `REST` (REST resources only) or `MIXED` (REST joined with DUCK or FUSION tables) |
| REST API | for a REST import: the API family and version it was read with |
| suggested key | for a plain projection of one REST resource that keeps its whole key: that key (used by `DIFF` when no KEY is given) |

The table also gets a SQL comment (`COMMENT ON TABLE`), so clients that show comments display the origin in the tree.

```sql
DUCK TABLES
```

returns `TABLE_NAME, FUSION_HOST, IMPORTED_AT, ROW_COUNT, SOURCE_SQL, KEY_COLUMNS, WATERMARK_COLUMN, WATERMARK_VALUE,
DECODE, SOURCE_TYPE, REST_API, SUGGESTED_KEY, DECODED_AT, HISTORY_COMPLETE` for the tables of schema DUCK. Tables you created by hand have empty
origin fields; a table dropped by hand disappears from the list.

- `CREATE TABLE DUCK.x AS …` registers the table — from Fusion or from REST (`… FROM REST.invoices`); `CREATE OR
  REPLACE` registers it afresh and forgets refresh settings. A failed import registers nothing; `IF NOT EXISTS` on an
  existing table changes nothing (and reads nothing). `execute` and `executeUpdate` return the row count, `executeQuery`
  a one-row `ROWS_IMPORTED`, for both kinds.
- `CREATE OR REPLACE TABLE DUCK.x (…)`, `CREATE OR REPLACE TABLE DUCK.x AS SELECT … FROM DUCK.y` or `DROP TABLE DUCK.x`
  forget the recorded origin: the old source SQL no longer describes the table.
- With `duckTransactions=true` in manual-commit mode the registration is part of the same local transaction: a rollback takes back table and row.
- `INSERT INTO DUCK.x SELECT …` keeps the original source SQL and updates the time and row count.
- The workspace is shared by all environments **on purpose**; the registry is what tells a DEV table from a PROD one.

## 7. `REFRESH`: full and incremental

```sql
REFRESH DUCK.x                                              -- reload (or delta, once configured)
REFRESH DUCK.x KEY (id) SINCE (last_update_date)            -- configure and run a delta
REFRESH DUCK.x KEY (id, line) SINCE (last_update_date) OVERLAP '1 day'
REFRESH DUCK.x FULL                                         -- reload everything, keeping KEY/SINCE
REFRESH DUCK.x SNAPSHOT                                     -- take a snapshot first (section 9)
```

The result has one row: `TABLE_NAME, MODE (FULL/INCREMENTAL), ROWS, INSERTED, UPDATED, WATERMARK, SECONDS`.

**Safety rules** (both modes):

- The table must have been imported from Fusion (it needs its source SQL). A hand-made table is refused with the advice
  to create it with `CREATE TABLE DUCK.x AS SELECT …`.
- It is refused when the table was imported from **another host** than the connection's: `DUCK.x was imported from
  fusion-dev…; this connection is to fusion-prod…`. Use `CREATE OR REPLACE TABLE DUCK.x AS …` to import again from the
  new environment on purpose. A DEV table can never be overwritten with PROD data by accident.

**Full reload.** The source SQL runs again into a temporary table (a REST import reads its resources again the same
way the import did — never through Fusion — and is always reloaded in full; `KEY`/`SINCE` are refused for it); only when that has completely arrived is the old
table replaced, in one local transaction (see section 2). A lost connection, a cancel or an error leaves the old data
untouched, and the temporary table is removed. `DECODE` columns (section 8) are rebuilt on the new rows.

**Incremental.** With `KEY` (the columns that identify a row) and `SINCE` (a "last changed" column) only changed rows
travel:

1. The lower bound is the stored **watermark** minus the overlap. First run: the newest value of the column already in
   the table (an empty table is reloaded fully instead).
2. Fusion runs `SELECT * FROM (<source SQL>) WHERE <since> >= TO_TIMESTAMP('<bound>', …) ORDER BY <since>, <keys>`.
   The `ORDER BY` matters: pages are separate requests and without an order a page boundary on changing data loses or
   repeats rows.
3. The delta lands in a temporary table; if the same key appears twice, the newest version wins.
4. `MERGE INTO` updates matching keys and inserts new ones, casting to the table's column types.
5. The watermark moves to the newest `SINCE` value **of the rows just read** (not the local clock, so a laptop clock
   or time zone cannot skip changes), the registry row is updated, all in one transaction.

`OVERLAP` re-reads a margin before the watermark so rows committed late are not missed; units `second`, `minute`,
`hour`, `day` (`'30 minutes'`, `'2 days'`), default `1 hour`. Re-reading is harmless: a row already merged just updates
to itself. `KEY` and `SINCE` are remembered, so later a bare `REFRESH DUCK.x` is a delta. The watermark has a
one-second resolution; the overlap covers the rest.

**Which sources can be refreshed incrementally.** The delta wraps the source SQL, so it is complete only when every
row of the source stands on its own. `KEY … SINCE …` (or stored settings) is refused before Fusion is asked when the
source SQL, at any depth, has a row limit (`FETCH FIRST/NEXT`, `OFFSET`, `ROWNUM`, `LIMIT`, `SAMPLE`), an aggregate
(`GROUP BY`, `HAVING`, `COUNT/SUM/MAX…(`, `DISTINCT`), an analytic function (`OVER (`), a set operation (`UNION`,
`INTERSECT`, `MINUS`), `CONNECT BY` or `PIVOT/MODEL`: the watermark would be applied to rows already cut or combined
and a delta could succeed while missing changes. The driver never removes a limit or an `ORDER BY` from your SQL; the
message suggests `REFRESH DUCK.x FULL` (it reloads with the source's own semantics) or importing a plain query to
refresh incrementally. `FULL` also works for a table whose `KEY`/`SINCE` are stored: they stay, and the watermark
restarts from the reloaded rows.

**What a delta does not see.** `MERGE` only adds and updates: rows deleted in Fusion, and rows that no longer match
the source's `WHERE`, stay in the local table. Every incremental refresh says so in a warning; run `REFRESH DUCK.x
FULL` now and then.

A delta that finds nothing leaves the table and watermark alone. Columns are checked before Fusion is asked: a `KEY` or
`SINCE` column that is not in the table, or `KEY` without `SINCE`, is an error before Fusion is asked.

**Limits to know.**

- **Deletes** in Fusion are not noticed by a delta (there is nothing to read). Reload fully now and then, or compare
  counts.
- **Views and joins:** `LAST_UPDATE_DATE` of one table does not reflect changes in the others behind a view or join.
  Use a full reload, or reload a period.
- The delta wraps your source SQL, so it must still be under 32,767 bytes with the wrapper.
- A watermark is not a point-in-time snapshot: rows changing while pages are fetched can show up in the next delta.
  For reconciliations keep the interval you read.

## 8. Readable imports: `DECODE`

End a `CREATE TABLE … AS` with `DECODE` and the new table gets extra columns to the **right** of the imported ones; the
imported columns are untouched.

```sql
CREATE TABLE DUCK.inv AS
  SELECT invoice_id, invoice_type_lookup_code, vendor_id, org_id, attribute_category,
         attribute1, attribute2, attribute3, last_update_date
    FROM ap_invoices_all WHERE invoice_date > DATE '2026-01-01'
  DECODE FLEXFIELDS, NAMES, LOOKUP invoice_type_lookup_code AS 'INVOICE TYPE';
```

The clause is `DECODE` followed by any of `FLEXFIELDS [OF <table or alias>]`, `NAMES`, `ACCOUNTS`,
`LOOKUP <column> AS '<type>'`, separated by commas
(several `LOOKUP`s allowed). Oracle's own `DECODE(a, 1, 'x')` function is never mistaken for it. `INSERT … DECODE` is
refused. **Each decoder is optional:** if one cannot run (no such column, no access to a view) a warning says why and the
import and the other decoders stand.

**`LOOKUP col AS 'TYPE'`** adds `col_meaning`. The meanings come from the lookup type's values (`FND_LOOKUP_VALUES_VL`),
one meaning per code (when a code exists in several sets, the alphabetically first meaning). Codes without a match stay empty.

**`NAMES`** adds the name for the ids it knows, asking Fusion only for the ids present (at most 1000 per request; more
than 5000 distinct ids reads the whole small reference table, up to 200,000 rows):

| Column in your import | Added column | Read from |
|---|---|---|
| `VENDOR_ID` | `vendor_name` | `POZ_SUPPLIERS_V` |
| `ORG_ID`, `BU_ID` | `business_unit` | `FUN_ALL_BUSINESS_UNITS_V` |
| `LEDGER_ID`, `SET_OF_BOOKS_ID` | `ledger_name` | `GL_LEDGERS` |
| `PARTY_ID` | `party_name` | `HZ_PARTIES` |
| `CUST_ACCOUNT_ID` | `account_number` | `HZ_CUST_ACCOUNTS` |
| `PERSON_ID` | `person_name` | `PER_PERSON_NAMES_F` (global name, current date) |
| `INVENTORY_ITEM_ID` | `item_number` | `EGP_SYSTEM_ITEMS_B` |

A column that is not in your `SELECT`, or whose added name already exists, is skipped.

**`ACCOUNTS`** adds, for each `…CODE_COMBINATION_ID` or `…_CCID` column, the account combination as text —
`code_combination_id` → `account`, `dist_code_combination_id` → `dist_account` — with the segments of the row's
chart of accounts in their order and joined by its delimiter (`01-4100-000`). Fusion has no `GL_CODE_COMBINATIONS_KFV`;
the driver reads `GL_CODE_COMBINATIONS.SEGMENTn` for the ids present and the structure of each chart from the `GL#` key
flexfield (`FND_KF_STR_INSTANCES_B`, `FND_KF_STRUCTURES_B`, `FND_KF_SEGMENT_INSTANCES`, `FND_KF_SEGMENTS_B`; checked on a
live instance: `CHART_OF_ACCOUNTS_ID` is the structure instance). An id that is not found stays empty.

**`FLEXFIELDS`** decodes the table of a query over **one** table; for a join name the table, `FLEXFIELDS OF h` (its
alias or name). The driver finds the descriptive flexfields on that table and adds a
column per **segment**, named after the segment (`Project Code` → `project_code`; punctuation becomes `_`). The value is
copied from the segment's `ATTRIBUTEn` column, and the column gets a comment `DFF <code>: ATTRIBUTE1, ATTRIBUTE2`.

- A segment of a **context** is filled only in the rows of that context (the value of `ATTRIBUTE_CATEGORY`, or
  `GLOBAL_ATTRIBUTE_CATEGORY` for the `JG_*` flexfields) — so include the category column and the `ATTRIBUTEn` columns
  you want decoded in your `SELECT`. Segments in the global context (`Global Data Elements`) are filled in every row.
- The same segment name in several contexts becomes **one** column filled from each context's `ATTRIBUTEn`.
- A segment whose `ATTRIBUTEn` is not in your import is skipped. If the name collides with an existing column the new
  one gets `_dff`.
- A result column counts as the table's `ATTRIBUTEn` only when the query shows it is — `h.attribute1`, `attribute1`
  from the one table, `h.*` — not because of its name: in `SELECT h.invoice_id, v.attribute1 … DECODE FLEXFIELDS OF h`
  the vendor's `attribute1` is not decoded. If DuckDB's parser cannot read the query, a single-table query is matched
  by name and a join is refused.
- Verified on a live instance: 11 segments of `AP_INVOICES_ALL` (contexts and global) arrive as columns.

`REFRESH` decodes again after each load (the clause is kept in the registry), so a refreshed table has the same
columns. Each refresh therefore asks Fusion once more for the lookup, the names and the flexfield — a few seconds even
for a delta of one row.

## 9. Snapshots and `DIFF`

Fusion keeps no history of most setup data — profile options, lookups, value sets, approval rules, DFF definitions.
Quarterly updates and P2T refreshes change them without notice. A **snapshot** is a dated copy of a DUCK table;
`DIFF` compares two versions, or two tables.

```sql
SNAPSHOT DUCK.profile_options LABEL 'before 26D';       -- TABLE_NAME, SNAPSHOT_TS, LABEL, ROWS
SNAPSHOTS;  SNAPSHOTS DUCK.profile_options;             -- list: table, time, label, rows (newest first)
SNAPSHOTS DROP DUCK.profile_options;                    -- forget the versions
REFRESH DUCK.profile_options SNAPSHOT;                  -- snapshot, then reload
```

**Storage.** One hidden table per DUCK table, `__ofjdbc_snap_<name>`, holding every version (the table's columns plus
`__snapshot_ts` and `__snapshot_label`). A snapshot is a **full copy**, so mind the disk for big tables. If the table's
columns changed since the first snapshot, a new snapshot is refused with the advice to `SNAPSHOTS DROP` and start
over. The time is the **local clock** — when you took it, not the time of the data in Fusion.

```sql
DIFF DUCK.opts KEY (name, level);                                   -- latest snapshot  vs  now
DIFF DUCK.opts KEY (…) SINCE '2026-10-01';                          -- newest snapshot at or before the date  vs  now
DIFF DUCK.opts KEY (…) BETWEEN LABEL 'before 26D' AND LABEL 'after 26D';
DIFF DUCK.opts KEY (…) BETWEEN '2026-10-01' AND '2026-10-20';       -- two snapshots, by time
DIFF DUCK.lookups_prod WITH DUCK.lookups_test KEY (lookup_type, lookup_code);   -- two tables (DEV/PROD)
```

A time refers to the newest snapshot at or before it; a label to the newest snapshot with that label. `KEY` names the
columns that identify a row; it can be omitted for a table whose key was set by an incremental `REFRESH` (or, for a
table imported from one REST resource, the resource's key). A `KEY` whose values repeat on a side is refused — such rows
could not be paired — and a `NULL` in a key matches a `NULL` on the other side.

**Result:** `CHANGE, <key columns>, COLUMN_NAME, OLD_VALUE, NEW_VALUE`.

| CHANGE | Meaning | COLUMN_NAME / values |
|---|---|---|
| `ADDED` | key only in the newer side | empty |
| `REMOVED` | key only in the older side | empty |
| `CHANGED` | same key, different value | one row **per changed column** with old and new value |

Values are compared as text and `NULL` against a value counts as a change. Both sides must have the same columns (a
different set is an error that lists the difference); a `KEY` that is not a column is an error. Sorting and filtering
the result is done by the client.

## 10. `HISTORY`: results you can look at again

A slow query's result vanishes when you close the tab. With history on, the rows of every Fusion query you run are also
kept in the workspace, so you can look at them again — tomorrow, offline — without asking Fusion.

**Switch on** per connection with `?history=true` (property `ofjdbc.history`, `-Dofjdbc.history`, `OFJDBC_HISTORY`). Off
by default. `?historyMaxRows=` (default 1,000,000) caps the rows kept in total; the oldest queries are dropped first.

```sql
HISTORY                          -- ID, STARTED_AT, ROWS, COMPLETE, FUSION_HOST, SQL_TEXT; newest first (LAST n, default 50)
HISTORY SHOW 12                  -- the rows of entry 12, as a result set
SELECT * FROM DUCK.__ofjdbc_h12  -- the same rows, as a table you can filter and join (all text)
HISTORY KEEP 12 AS DUCK.invoices_oct   -- turn the entry into a typed, registered table
HISTORY CLEAR
```

**What is recorded.** Your own queries to Fusion — including ones with their own `FETCH FIRST`, `OFFSET` or `ROWNUM`.
Not recorded: the driver's internal queries (metadata, dictionary lookups, imports, `DECODE`), DUCK statements and
driver commands. DBeaver's **next result page** of the same query continues the same entry instead of creating a new
one, and a page prefetched while you read is included. Running the query again from the start (Refresh) makes a new
entry.

- `COMPLETE` is true when the last page was read; closing the tab early leaves `false` and the rows read so far.
- A query is recorded up to 100,000 rows (the same limit as the next-page cache); beyond that the entry stays
  `COMPLETE = false`.
- Rows are stored as text (an archive, not a data mart). `HISTORY KEEP` infers types like an import does, registers the
  table with the origin of the entry, and is the way to make an entry permanent. `HISTORY KEEP 12 AS DUCK.x DECODE …`
  adds the `DECODE` columns (section 8) — with **today's** names and meanings, not those of when the rows were read: a
  warning says so, `DUCK TABLES` shows `DECODED_AT` and `HISTORY_COMPLETE` (`N` when the entry holds only the pages
  shown), and the table comment repeats both.
- History needs the next-page cache (`nextPageCacheSeconds` not `0`).

**Privacy.** The rows are written into the workspace file on your disk, which is shared by all your connections. If
queries return sensitive data, switch history on only for the connection that needs it, and `HISTORY CLEAR` when done.

## 11. `JOIN KEYS` and join paths

Fusion tables carry no foreign keys you can query, but Oracle's own views show how they relate. The driver reads the text
of every view in `FND_VIEWS` once, keeps each join between real tables (the condition and the views that use it) and
stores the result locally.

```sql
JOIN KEYS AP_INVOICES_ALL                              -- everything it joins to, most used first
JOIN KEYS AP_INVOICES_ALL AP_INVOICE_LINES_ALL         -- the key between two tables
JOIN KEYS REFRESH                                      -- rebuild the index from Fusion
```

The result has `PATH, STEP, TABLE_NAME, RELATED_TABLE, JOIN_CONDITION, VIEW_COUNT, EXAMPLE_VIEWS`; direct keys are
numbered as paths of one step. Names may be quoted patterns (`'AP_INV%'`).

**Paths.** When two tables have **no key of their own**, the driver searches for a way through up to two intermediate
tables, one row per step:

```sql
JOIN KEYS AP_INVOICES_ALL GL_JE_LINES
-- path 1: AP_INVOICES_ALL → AP_INVOICE_DISTRIBUTIONS_ALL → GL_CODE_COMBINATIONS → GL_JE_LINES   (views per step: 7, 1, 2)
-- path 2: AP_INVOICES_ALL → XLE_ENTITY_PROFILES → GL_LEDGER_LE_V → GL_JE_LINES                    (2, 1, 1)
-- path 3: AP_INVOICES_ALL → AP_CHECKS_ALL → CE_RECON_HISTORY_ITEMS → GL_JE_LINES                  (1, 1, 1)
```

- Each hop uses the most used key of that pair of tables.
- Up to five paths, best first: the one whose **weakest** step is used by the most views, then the shortest, then the
  one used by the most views overall.
- Tables joined to more than 40 others (lookups, organisation units, people) would connect everything, so they are
  never a stepping stone; the warning says how many were skipped.
- It is a lead, not proof: it shows which joins Oracle itself uses. Check the cardinality of each step before you build a
  report on it. Output on your instance depends on its views, so results differ between releases.
- The first call builds the index from all views (about ten seconds on the dev instance tested, longer on a big one); afterwards it is answered locally in milliseconds.

## 12. DuckDB 2.0 and your existing files

The driver embeds DuckDB 2.0, whose files use a newer storage format.

- **Existing files need nothing.** `duck.duckdb` and `metadata.db` written by DuckDB 1.x open as they are and keep their
  old format as long as you use them (verified on real files).
- **A file created by 2.0 cannot be read by 1.x.** An older driver build, or an older DuckDB command-line shell, fails on
  it with "database file with version number 69 … created with a newer version of DuckDB". Other tools that open
  `~/.ofjdbc/duck.duckdb` directly need DuckDB 2.x for a file made by this version.
- **If the driver cannot read a file it does not fail the connection;** it says so — in the log, as a warning on the
  connection and on the next statement:
  - **`metadata.db`** is only a cache of Fusion's dictionary: it is **deleted and rebuilt** as you browse. Nothing to do.
  - **`duck.duckdb`** holds your tables, so it is **moved** to `duck.duckdb.unreadable-<date>` beside it and a new empty
    workspace is created. To get the tables back, open the old file with the DuckDB version that wrote it (for example
    its command-line shell) and export them; delete the file when you do not need it.
- **To go back to an older driver build on purpose:** delete `~/.ofjdbc/metadata.db*` and move `duck.duckdb` away first.
- After upgrading, delete `~/.ofjdbc/metadata.db*` once if navigator types look wrong: column types of Fusion tables
  with a `TIMESTAMP` column (such as `LAST_UPDATE_DATE`) were cached as text by older builds.

## 13. Command summary

| Command | What it does | Section |
|---|---|---|
| `DUCK TABLES` | tables of schema DUCK with their origin | 6 |
| `REFRESH DUCK.x [KEY (…) SINCE (…)] [OVERLAP '…'] [FULL] [SNAPSHOT]` | reload, or merge what changed | 7 |
| `CREATE TABLE DUCK.x AS … DECODE FLEXFIELDS [OF t], NAMES, ACCOUNTS, LOOKUP c AS 't'` | import with readable columns | 8 |
| `SNAPSHOT DUCK.x [LABEL '…']`, `SNAPSHOTS [DUCK.x]`, `SNAPSHOTS DROP DUCK.x` | versions of a table | 9 |
| `DIFF DUCK.x [WITH DUCK.y] [KEY (…)] [SINCE '…' \| BETWEEN … AND …]` | what changed | 9 |
| `HISTORY [LAST n]`, `HISTORY SHOW n`, `HISTORY KEEP n AS DUCK.x [DECODE …]`, `HISTORY CLEAR` | kept results | 10 |
| `… col IN (SELECT col FROM DUCK.t)` inside a Fusion subquery | send local keys to Fusion | 4 |
| `#d COPY (SELECT … FROM FUSION.x) TO 'file'` | export to Parquet/CSV | 5 |
| `JOIN KEYS A [B]`, `JOIN KEYS REFRESH` | how tables join, paths | 11 |

Settings: `?duckDb=`, `?history=`, `?historyMaxRows=`, `?nextPageCacheSeconds=` — see
[Environment Variables](environment-variables.md).

## 14. Messages and what to do

| Message (shortened) | Meaning and what to do |
|---|---|
| `The Fusion query returned no rows, so its columns are unknown` | `CREATE TABLE … AS` needs at least one row to learn the columns. Create the table first, then `INSERT INTO … SELECT`. |
| `DUCK.x already exists (use CREATE OR REPLACE TABLE or INSERT INTO)` | Choose replace or append. |
| `Table with name x does not exist … DUCK.x is not visible to this connection` | With `duckTransactions=true`: the table was created in another connection — DBeaver opens one per SQL editor — that has not committed. Commit in that editor, refresh, try again. |
| `An earlier DUCK statement failed inside the open transaction (…)` | With `duckTransactions=true` in manual-commit mode (perhaps set by DBeaver's Smart commit): the error in brackets aborted the transaction. Rollback or Auto-commit, then run again (section 2). |
| `DUCK.x was not imported from Fusion (no source SQL recorded)` | `REFRESH` works on tables created by an import. |
| `DUCK.x was imported from <host>; this connection is to <other>` | Protection against mixing environments. `CREATE OR REPLACE TABLE DUCK.x AS …` to import from this one. |
| `An incremental REFRESH needs both KEY (…) and SINCE (…)` | Give both columns. |
| `The key values from DUCK make the Fusion query N bytes long` | Too many keys for one query. Narrow `DUCK.ids`, or import the Fusion table and join locally. |
| `More than 50000 key values from DUCK …` | Same; import the table instead. |
| `DECODE <part> skipped: …` | One decoder could not run; the import stands. The text says why (missing column, no access, unknown lookup type — `LOOKUP '<type>'` shows similar types). |
| `DUCK.x has no snapshots` / `has other columns now than when it was first snapshotted` | Take a snapshot first, or `SNAPSHOTS DROP DUCK.x` and start again. |
| `No history entry N` | `HISTORY` lists the entries; history is recorded only with `?history=true`. |
| `reused N part(s) fetched from Fusion by the previous run` | Not an error: the next result page reused the earlier fetch. |
| `The DUCK workspace … was written by a different DuckDB version …` | Section 12: the old file was moved aside, a new workspace created. |
| `DUCK workspace … is unavailable (it may be open in another program)` | Another process holds the file. Close it, or use `?duckDb=<other file>`. |
