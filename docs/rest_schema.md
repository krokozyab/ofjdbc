# REST Schema Reference

The `REST` schema shows the REST resources of your Oracle Fusion instance as tables you can browse in the navigator,
query with SQL, join with `DUCK` and `FUSION` data, and import into the `DUCK` workspace. This page holds every rule,
limit and message; the [README](../README.md#rest-schema-browse-and-query-fusion-rest-resources) has the short tour.

## Contents

1. [Basics](#1-basics)
2. [Queries](#2-queries)
3. [Child resources](#3-child-resources)
4. [Finders](#4-finders)
5. [Imports into DUCK and REFRESH](#5-imports-into-duck-and-refresh)
6. [Commands](#6-commands)
7. [Settings](#7-settings)
8. [Messages and what to do](#8-messages-and-what-to-do)

## 1. Basics

- **What you see.** Every resource of the REST API (default family `fscmRestApi`) is a table; its attributes are the
  columns, with type, size, mandatory flag and the title as the column comment. The resource's title is the table
  comment. Expanding `REST` → Tables lists all resources — the first time takes a few seconds, then the list is
  remembered on your computer, as is each resource's description once you open it.
- **Read-only.** The driver only reads. Nothing is ever created, changed or deleted in Fusion through `REST`.
- **Access.** REST uses the connection's own sign-in (password, browser SSO or OAuth). Which resources and rows a user
  may read is decided by their Fusion roles. A resource can show its description and still refuse its rows — see
  [`403`](#8-messages-and-what-to-do).
- **Not the same as `FUSION`.** `REST.invoices` and `FUSION.AP_INVOICES_ALL` are different interfaces to the same
  application: names of fields, which rows and fields you get, and what your roles let you read can differ.
- **Column types.**

| REST type | Column type |
|---|---|
| string | `VARCHAR` |
| integer | `BIGINT` |
| number | `DECIMAL(38,10)` |
| date | `DATE` |
| datetime | `TIMESTAMP WITH TIME ZONE` |
| boolean | `BOOLEAN` |
| anything else (attachments, …) | shown, but not read |

- **SQL dialect.** A statement reading `REST` runs in the embedded DuckDB engine, so it is written in DuckDB SQL
  (`LIMIT`, `ILIKE`, `::` casts, …), not in Oracle SQL. Quote names that have upper-case letters if you like —
  `REST."ledgerBalances"` — names are matched ignoring case either way.

## 2. Queries

```sql
SELECT InvoiceNumber, InvoiceAmount FROM REST.invoices WHERE InvoiceAmount > 1000 AND InvoiceCurrency = 'EUR' LIMIT 50;
SELECT r.InvoiceNumber, d.tag FROM REST.invoices r JOIN DUCK.tags d ON d.id = r.InvoiceId;
SELECT InvoiceCurrency, count(*), sum(InvoiceAmount) FROM REST.invoices WHERE InvoiceDate >= DATE '2024-01-01' GROUP BY 1;
```

**The whole `WHERE` is always honoured.** Some conditions are also applied by Fusion, so fewer rows travel; whatever
Fusion cannot apply is applied locally, and nothing is silently dropped.

**Conditions that make a read faster** — on attributes the description marks queryable (`DESCRIBE REST x`,
column `QUERYABLE`), joined by `AND`:

| Condition | Applied by Fusion when |
|---|---|
| `=`, `<>`, `<`, `>`, `<=`, `>=` with a number or a date | always |
| `=`, `<>` with text, `IN ('a', 'b')`, `IN (1, 2)` | the text is not empty and has no quote |
| `IS NULL`, `IS NOT NULL` | always (see joins below) |
| `LIKE`, `NOT LIKE` with `%` and `_` | the pattern has no `*` or `\` and no `ESCAPE` |

Text comparisons are case-sensitive, as in DuckDB. `OR`, functions (`lower(…)`), `ILIKE` and conditions between two
columns are applied locally only.

**Only the columns you name are read.** `SELECT *` reads every readable attribute.

**Row limit.** A plain `SELECT … FROM REST.x [WHERE …] [LIMIT n]` reads at most the rows it needs — the `LIMIT`, or the
result size your client asks for (DBeaver's "Result Set" size). The driver stops a read that exceeds `restMaxRows`
(default 50,000), before it starts when Fusion reports the count, otherwise as soon as it is passed; a `LIMIT` above the
setting does not lift it.

**Joins.**

- Conditions of the `SELECT` that concern only the resource still make its read faster; qualify its columns with its
  alias there (`WHERE i.InvoiceStatus = 'Validated'`) — an unqualified column could belong to the other table.
- `IS NULL` on a resource that is the outer side of a `LEFT`/`RIGHT`/`FULL JOIN` is applied locally only: it is also
  true for the rows the join adds.
- Joined to a DUCK table on a queryable attribute by an inner join (or a `LEFT JOIN` from the DUCK table), the resource
  is read only for the DUCK table's distinct keys; an empty DUCK table reads nothing. With more than `restMaxKeys` keys
  (default 1,000), or keys that do not fit the attribute's type, the resource is read without them and a warning says
  so.
- A `LIMIT` is never applied before a join or a local filter.

**Next result page.** DBeaver runs a statement again for its next page; the rows already read from REST are reused for
a few minutes (the same time as for Fusion pages), so scrolling does not read REST again.

## 3. Child resources

Child resources — an invoice's lines, a line's distributions, flexfields, attachments — are tables named by their path:
`invoices__invoiceLines`, `invoices__invoiceLines__invoiceDistributions`. Their first columns are the keys of their
parents: `__parent_InvoiceId` for the top resource, `__parent_invoiceLines_LineNumber` for a level in between.

**A child is always read for given parents**, never for all of them. Opening one without a parent filter returns an
explanation. Parents come from:

```sql
-- the WHERE: = or IN on the first parent key
SELECT * FROM REST.invoices__invoiceLines WHERE __parent_InvoiceId IN (300000000200001, 300000000200002);
-- a DUCK table joined on it (LEFT JOIN keeps parents without lines)
SELECT p.InvoiceId, l.LineNumber, l.LineAmount
  FROM DUCK.selected_invoices p LEFT JOIN REST.invoices__invoiceLines l ON l.__parent_InvoiceId = p.InvoiceId;
-- another REST resource of the statement
SELECT i.InvoiceNumber, l.LineAmount
  FROM REST.invoices i JOIN REST.invoices__invoiceLines l ON l.__parent_InvoiceId = i.InvoiceId
 WHERE i.InvoiceDate >= DATE '2024-10-01';
-- a parenthesized FUSION subquery: read first, in Oracle SQL as written
SELECT f.invoice_id, l.LineNumber
  FROM (SELECT invoice_id FROM FUSION.ap_invoices_all WHERE invoice_num LIKE 'INV-10%') f
  JOIN REST.invoices__invoiceLines l ON l.__parent_InvoiceId = f.invoice_id;
```

Other parent sources (a CTE, a subquery reading DUCK, a `FUSION.x` table joined without parentheses) are refused with an
explanation: import the parents into DUCK first.

- Children are fetched in batches where supported; speed depends on the resource and data volume.
- `restMaxParents` (default 200) caps the parents and `restMaxRequests` (default 2,000) the requests of one statement;
  both are checked before or while reading, and a cancel stops the statement between requests.
- Conditions on the child's own queryable attributes make the read faster, as for a resource.
- A resource whose key has more than one attribute does not offer child tables yet.

**In the navigator** the child tables appear right below their resource once its description has been read (expand
the resource's Columns, or run `DESCRIBE REST invoices`) and the table list is refreshed (F5):

| Where | What you see |
|---|---|
| Comment of a resource | its title and all its child resources |
| Comment of a child table | which resource it belongs to |
| Keys of a child table | the parents' keys plus its own — a line number is unique only within its invoice |
| Foreign Keys of a child table | the link to the table one level up (also in ER diagrams) |

A level without a key of its own in Fusion has no key here either, and its children then have none.

## 4. Finders

A finder is a named search of a resource, as in the Fusion UI. `REST_FIND` reads a resource through one:

```sql
DESCRIBE REST FINDER ledgerBalances AccountGroupBalanceFinder;
SELECT * FROM REST_FIND('ledgerBalances', 'AccountGroupBalanceFinder',
    ledgerName := 'Vision Operations', accountGroupName := 'Cash', accountName := 'Cash',
    accountingPeriod := 'Jan-24', currency := 'USD');
```

- `DESCRIBE REST x` lists a resource's finders; `DESCRIBE REST FINDER x f` lists the parameters with their type, marks
  the required ones `REQUIRED = Y` and ends with a call to copy (replace the sample values, drop the optional ones).
- Some resources (`ledgerBalances`, …) are read **only** through a finder: a plain `SELECT` fails with the finders to
  use and an example call, using the finder that needs the fewest values.
- The call is checked before anything is sent: a missing required parameter, an unknown finder or parameter, and a
  value of the wrong type are reported. Types: a number for integer and number, `'yyyy-mm-dd'` or `DATE 'yyyy-mm-dd'`
  for a date, `true`/`false` for a boolean. Values must be constants; other casts are refused.
- Values containing `,` `;` `=` or `"` cannot be passed yet.
- A finder selects the rows itself: a `WHERE` on top narrows the result locally. The result joins, aggregates and
  imports like any table.

## 5. Imports into DUCK and REFRESH

```sql
CREATE TABLE DUCK.inv AS SELECT * FROM REST.invoices WHERE InvoiceDate >= DATE '2024-01-01';
REFRESH DUCK.inv;
```

- An import from REST is registered like one from Fusion: `DUCK TABLES` shows the host, the source SQL, the time and row
  count, `SOURCE_TYPE` (`REST`, or `MIXED` when it also reads DUCK or FUSION) and `REST_API` (family and version).
- `CREATE OR REPLACE`, `IF NOT EXISTS` and `INSERT INTO DUCK.x SELECT … FROM REST.y` behave as for Fusion imports;
  `IF NOT EXISTS` on an existing table reads nothing. `executeUpdate` returns the row count, `executeQuery` a one-row
  `ROWS_IMPORTED`.
- `REFRESH DUCK.x` reads REST again **in full** and swaps the table in one step; a failed read keeps the old rows.
  `KEY`/`SINCE` (incremental refresh) is not available for REST imports.
- A table imported as a plain projection of one resource that keeps its whole key (`SELECT *` or the key columns under
  their own names, no join, grouping or rename) gets that key as `SUGGESTED_KEY`; `DIFF` uses it when no `KEY` is given.
- `REFRESH` of a REST import is refused on a connection whose `restApi` differs from the one the table was read with.
- Replacing or dropping the table by other DDL forgets its origin.

## 6. Commands

| Command | Result |
|---|---|
| `REST RESOURCES ['pattern']` | `RESOURCE, DESCRIBED, TITLE, USAGE, ATTRIBUTES` — resources whose name contains the word, or matches a `%`/`_` pattern |
| `DESCRIBE REST x` | `ATTRIBUTE, TYPE, SIZE, MANDATORY, QUERYABLE, UPDATABLE, LOV, PK, TITLE`, then rows `child:…` and `finder:…(params)` |
| `DESCRIBE REST FINDER x f` | `PARAMETER, TYPE, REQUIRED`, then an `EXAMPLE` row with a call to copy |
| `REFRESH REST x` | reads the description again: `RESOURCE, ATTRIBUTES, CHANGED` (`Y` when it changed) |
| `REFRESH REST RESOURCES` | reads the list again: `RESOURCES, ADDED, REMOVED` |

A description that Fusion refused (not found, no access) is remembered for an hour, so the navigator does not ask again
on every click; `REFRESH REST x` asks again at once. The list is kept per host and API version, the descriptions also per user.

## 7. Settings

URL parameter (`?rest=false`), connection property `ofjdbc.<name>`, system property `-Dofjdbc.<name>` or environment
variable `OFJDBC_<NAME>` (`OFJDBC_REST_MAX_ROWS`, …):

| Setting | Default | Meaning |
|---|---|---|
| `rest` | `true` | `false` hides the `REST` schema |
| `restApi` | `fscmRestApi` | the API family |
| `restVersion` | `latest` | the API version; `latest` is the newest version Fusion reports |
| `restMaxRows` | `50000` | most rows one statement may read from a resource |
| `restMaxParents` | `200` | most parents a child table is read for in one statement |
| `restMaxRequests` | `2000` | most requests one statement may make for child tables or for the keys of a join |
| `restMaxKeys` | `1000` | most keys of a DUCK table a join sends with a resource read; with more it is read without them |

## 8. Messages and what to do

| Message (shortened) | Meaning and what to do |
|---|---|
| `REST GET … failed (403): access denied — the connection's user has no Fusion role …` | The user may read the description but not the rows. Ask your security administrator for a role, or read another resource with the same data. |
| `REST GET … failed (404): not found` | No such resource for this API family and version (`restApi`, `restVersion`). |
| `REST GET … failed (400): <Fusion's reason>` | Fusion refused the request; its own reason follows. |
| `REST.x can only be read through a finder: …` | Use `REST_FIND` with one of the listed finders — the message has an example ([section 4](#4-finders)). |
| `REST.x has no finder 'f'` / `Finder f of REST.x has no parameter 'p'` | The names listed in the message are the valid ones. |
| `Finder f of REST.x needs P1, P2` | Give the required parameters; `DESCRIBE REST FINDER x f` shows a call. |
| `REST_FIND: p is integer in the describe, 'v' is not` | Pass a value of the parameter's type. |
| `REST.x has N rows matching the filters sent to REST (limit …, setting restMaxRows)` | Add conditions on queryable attributes, a smaller `LIMIT`, or raise `restMaxRows`. |
| `REST.x returned more than N rows (setting restMaxRows)` | As above, found while reading. |
| `REST.x would need more than N requests (setting restMaxRequests)` | Fewer keys or parents, a narrower filter, or a higher `restMaxRequests`. |
| `REST.x is read per parent: add WHERE __parent_… = <id> …` | Give the parents ([section 3](#3-child-resources)). |
| `REST.x would be read for more than N parents (setting restMaxParents)` | Put the parents you need in a smaller DUCK table, or raise `restMaxParents`. |
| `The parents of REST.x would come from …` | Only a DUCK table, a REST resource or a parenthesized FUSION subquery can give parents; import the rest first. |
| `REST.x has no child resource 'y'; its children: …` | Use one of the listed children. |
| `Child resources of REST.x are not supported: its key is not a single attribute` | Not available yet for this resource. |
| `REST.x has no attribute of a readable type` | Its attributes are all of types that are not read (attachments, …). |
| `The statement reads REST, but DuckDB cannot parse it` | Statements reading `REST` are DuckDB SQL; fix the syntax (Oracle-only syntax is not accepted there). |
| `The REST schema is switched off for this connection (rest=false)` | Remove `rest=false`. |
| `DUCK.x was imported from REST; it is refreshed by reading it again in full` | Use `REFRESH DUCK.x` without `KEY`/`SINCE`. |
| Warning `… has more than N keys for REST.x (setting restMaxKeys); the resource is read without them` | The join still gives the right rows, only slower; narrow the DUCK table or raise `restMaxKeys`. |
| Warning `the keys in … do not all fit …; the resource is read without them` | The DUCK column holds values of another type than the attribute; the result is still right. |
