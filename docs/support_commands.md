# Troubleshooting and Configuration Commands — Reference

Commands for support engineers: why an import rejected rows, what a scheduled process wrote to its log, who changed a
record, which approvals are stuck, and how the instance is configured. Each is a statement you run in the SQL editor;
the answer is an ordinary result set you can sort, filter and export, and a short summary appears in the client's
Output panel (DBeaver: *Output* / *Execution log*). `HELP` lists every command with an example to copy.

## Contents

1. [Basics](#1-basics)
2. [`IMPORT ERRORS`: rows an import rejected](#2-import-errors-rows-an-import-rejected)
3. [`ESS REQUESTS`, `ESS REQUEST`, `ESS LOG`: scheduled processes](#3-ess-requests-ess-request-ess-log-scheduled-processes)
4. [`AUDIT`: who changed what](#4-audit-who-changed-what)
5. [`APPROVAL TASKS`: approvals waiting or failed](#5-approval-tasks-approvals-waiting-or-failed)
6. [Configuration checks: `FEATURES`, `BUSINESS EVENTS`, `ENDPOINTS`, `FLEXFIELD DEPLOYMENT`](#6-configuration-checks)
7. [The commands as tables in DUCK queries](#7-the-commands-as-tables-in-duck-queries)
8. [Command summary](#8-command-summary)
9. [Messages and what to do](#9-messages-and-what-to-do)

## 1. Basics

- Commands are not case-sensitive; text arguments are in single quotes (`'…'`, a quote inside doubled: `'it''s'`).
- A **pattern** matches case-insensitively. With `%` or `_` it is a `LIKE` pattern (`'AP%'`); without them it matches
  anywhere in the text (`'invoice'` is `'%INVOICE%'`).
- What a command can show is what the connection's user may see. `IMPORT ERRORS`, `ESS REQUESTS` and `ESS REQUEST`
  read Fusion tables like any query. `ESS LOG`, `AUDIT`, `APPROVAL TASKS` and the configuration checks read Fusion's
  REST APIs — they work also when the REST schema is switched off (`?rest=false`), but the user needs access to those
  APIs (each section says which). A refusal names the API and Fusion's reason.
- Each command is also a table function in DUCK queries (section 7), so its result can be filtered, joined, kept and
  compared between environments.

## 2. `IMPORT ERRORS`: rows an import rejected

```sql
IMPORT ERRORS 1234567              -- the errors of one import run (its ESS request id)
IMPORT ERRORS 1234567 FOR AP       -- only one interface
IMPORT ERRORS FOR CUSTOMERS BATCH '300000012345678'   -- a batch, for interfaces kept by batch
IMPORT ERRORS                      -- the errors now in every interface
```

One row per error, the same columns for every interface: `INTERFACE`, `SEVERITY`, `RECORD_TYPE` (header, line, site, …),
`DOCUMENT` (invoice, journal, supplier, order number …), `LINE`, `ERROR_CODE`, `MESSAGE` (the text, tokens filled in),
`COLUMN_NAME` and `VALUE` (the field that failed, when the interface says), `REQUEST_ID`, `BATCH_ID`,
`INTERFACE_TABLE`, `INTERFACE_ID` (the row in the interface table), `CREATED`.

| Interface | Import |
|---|---|
| `AP` | Payables invoices (Import Payables Invoices) |
| `AR` | Receivables transactions (AutoInvoice) |
| `GL` | Journals (Journal Import) — status codes explained with the Journal Import Execution Report texts |
| `SUPPLIERS` | Suppliers, sites, addresses, contacts, site assignments |
| `CUSTOMERS` | Customers and parties (Trading Community import batches) |
| `PO` | Purchase orders and agreements |
| `RECEIPTS` | Receiving transactions |
| `ITEMS` | Items (Product Hub item import) |
| `BANK_STATEMENTS` | Bank statements |
| `INVENTORY` | Inventory transactions |
| `CYCLE_COUNTS` | Cycle count entries |
| `SHIPPING` | Shipping transactions |
| `ASSETS` | Fixed asset mass additions |
| `PROJECTS` | Project costs |
| `INTERCOMPANY` | Intercompany transactions |
| `LEASES` | Leases |
| `CARDS` | Corporate card statements |

- **Which interfaces are read.** With a request id, the job names of the request and its child requests say which
  import it was. When they do not (an unknown job, or a request already purged from the scheduler's history), every
  interface is asked whether it holds rows of the request, and only those are read.
- **AutoInvoice** keeps no request id on its errors and rewrites them on each run: the errors of a run are those
  created while it ran, for its business unit and transaction source.
- **Customers** are imported in batches; with a request id the batch is taken from the job's parameters, or give
  `BATCH '…'`.
- No rows means the import had no errors, or its rows were corrected, imported again or purged since — the summary says
  which interfaces were read. An interface the user may not read is named in a warning; the others are still read.
- At most 10,000 rows; name an interface or a request to narrow.

## 3. `ESS REQUESTS`, `ESS REQUEST`, `ESS LOG`: scheduled processes

```sql
ESS REQUESTS 'ImportPayables%' LAST 20   -- latest requests of a job: state, submitter, times, error message
ESS REQUEST 1234567                      -- one request: parameters, message, properties, child requests
TIMELINE ESS 1234567                     -- the request and every request below it in time order: submitted, scheduled, started, ended
ESS LOG 1234567                          -- its log files and those of its child requests, one row per line
ESS LOG 1234567 OUT                      -- the output files instead (ALL: both)
```

`ESS LOG` columns: `REQUEST_ID` (the request whose file it is), `FILE_NAME`, `LINE`, `TEXT`. Binary files (PDF,
spreadsheets) are left out and named in the summary; at most 1,000,000 lines. It reads the REST resource
`erpintegrations` — the user needs the access used for importing data through ERP integrations. *No files* means the
request has not finished, its files were purged, the id is not a scheduled process, or the user may not see it.

To search a log, use it as a table: `SELECT * FROM ess_log(1234567) WHERE text ILIKE '%ORA-%'` (section 7).

`TIMELINE ESS` columns: `TIME`, `REQUEST_ID`, `PARENT_REQUEST_ID`, `DEPTH` (1 = the request asked for), `JOB_NAME`, `EVENT`
(`SUBMITTED`, `SCHEDULED` when it waited for a later time, `STARTED` with the seconds it waited, `ENDED` with state,
seconds, attempt and message, `NOT ENDED` with the state now), `STATE`, `DETAIL`. Fusion keeps one history row per request
with its final state, so the events are its time stamps — not every state it passed through. A recurring schedule's runs
are requests below it, so its timeline lists every run (at most 10,000 rows).

## 4. `AUDIT`: who changed what

```sql
AUDIT OBJECTS                         -- what is audited: objects with their tables and product
AUDIT OBJECTS 'supplier'
AUDIT POZ_SUPPLIERS                   -- changes to a table in the last 7 days
AUDIT POZ_SUPPLIERS DAYS 30
AUDIT POZ_SUPPLIERS FROM '2026-09-01' TO '2026-11-30' USER 'john.smith'
AUDIT AuditSupplierVO DAYS 30         -- an audited object with its child objects (sites, addresses, bank accounts …)
```

Fusion's audit history — what the *Audit Reports* page shows — one row per changed attribute, newest first:
`AUDIT_DATE` (UTC), `USER_NAME` (login), `USER_DISPLAY_NAME`, `EVENT` (`Insert`, `Update`, `Delete`),
`BUSINESS_OBJECT`, `OBJECT` (the record as Fusion describes it), `OBJECT_KEY`, `CONTEXT` (e.g. `Supplier Number=…`),
`ATTRIBUTE`, `ATTRIBUTE_NAME`, `OLD_VALUE`, `NEW_VALUE`, `OLD_VALUE_ID`, `NEW_VALUE_ID` (codes behind displayed values),
`TABLE_NAME`, `COLUMN_NAME` (where the attribute is stored), `IMPERSONATOR`, `OBJECT_TYPE`, `PRODUCT`.

- **What to name.** A table (`POZ_SUPPLIERS`) gives the changes of that table's columns only. An audited object — its
  short name from `AUDIT OBJECTS` (`AuditSupplierVO`) or its full name — gives its changes and, for a top object,
  those of its child objects. `AUDIT OBJECTS` shows each object's top object, tables and number of audited attributes.
- **Period.** `DAYS n` (default 7, ending today, UTC), or `FROM 'yyyy-mm-dd' [TO 'yyyy-mm-dd']`; at most 366 days.
  Fusion answers at most 30 days per request, so a longer period takes several requests (the summary counts them).
- **`USER 'login'`** filters by the sign-in name (`USER_NAME`), not the display name.
- **Product.** Fusion files audit history under a product (web application); the command finds it from the audit
  setup. If it cannot, it says so — give `PRODUCT '…'` (the `PRODUCT` column of `AUDIT OBJECTS`).
- **No rows** means nothing audited changed in the period, or audit was not on for the object then. Audit is turned on
  per object in *Setup and Maintenance → Manage Audit Policies*; `AUDIT OBJECTS` lists what is audited now.
- The user needs the privilege to view audit history (as for the *Audit Reports* page). At most 100,000 rows; shorten
  the period or give `USER` for busy objects.

## 5. `APPROVAL TASKS`: approvals waiting or failed

```sql
APPROVAL TASKS                                   -- tasks waiting for an approver, newest first
APPROVAL TASKS 'Invoice' WITH ASSIGNEES          -- titles containing the word, with who has each task
APPROVAL TASKS ERRORED                           -- also COMPLETED, ALERTED, SUSPENDED, EXPIRED, WITHDRAWN
APPROVAL TASKS ASSIGNED SINCE '2026-11-01' LAST 100
APPROVAL TASK 1234567                            -- one task with its current assignees
```

Columns: `TASK_NUMBER`, `TITLE`, `STATE`, `DAYS_WAITING` (days since an assigned task was assigned), `ASSIGNEES`,
`CATEGORY`, `TASK_DEFINITION` (the kind of approval, e.g. `FinApInvoiceApproval`), `CREATED`, `ASSIGNED`, `UPDATED`,
`CREATED_BY`, `FROM_USER`, `OUTCOME`, `PRIORITY`, `IDENTIFICATION_KEY` (often the document id), `OWNER`, `TASK_ID`,
`TASK_NAMESPACE`.

- The state is `ASSIGNED` unless given; the word searches task titles; `SINCE` keeps tasks created from that day.
- The newest 500 tasks unless `LAST n` (at most 10,000); the summary gives the total.
- The task list leaves out the assignees of many tasks. `WITH ASSIGNEES` reads each task for them (up to 200 tasks);
  `APPROVAL TASK n` reads one.
- **Whose tasks.** Every user's tasks are shown to a user who may administer workflow tasks. For other users the
  command shows their own and their groups' tasks and says so.
- The approval history of a task (who approved before, who is next) is not read — Oracle's API takes minutes for it;
  open the task in Fusion.

## 6. Configuration checks

```sql
FEATURES                        -- opt-in features that are enabled
FEATURES 'payables'             -- features whose code or name matches, enabled or not
FEATURES ALL                    -- every feature
BUSINESS EVENTS                 -- ERP business events that are enabled
ENDPOINTS                       -- topology endpoints that are set
ENDPOINTS 'payment'
FLEXFIELD DEPLOYMENT            -- descriptive and extensible flexfields not deployed
FLEXFIELD DEPLOYMENT 'AP_INVOICES'
```

The same rule for each: without a pattern, what usually matters; a pattern searches every row by code or name; `ALL`
lists every row.

| Command | Shows | Columns |
|---|---|---|
| `FEATURES` | Opt-in features (*Setup and Maintenance → Opt In*) and the options chosen | `FEATURE_CODE`, `FEATURE_NAME`, `ENABLED`, `SELECTED_CHOICES`, `FEATURE_TYPE`, `CLASSIFICATION`, `FROZEN`, `OBSOLETE` |
| `BUSINESS EVENTS` | ERP business events and whether they are raised — an integration waiting for an event that is off never hears of it | `EVENT_CODE`, `EVENT_IDENTIFIER`, `ENABLED`, `LAST_UPDATE_DATE`, `LAST_UPDATED_BY`, `CREATION_DATE`, `EVENT_ID` |
| `ENDPOINTS` | Where applications, modules and third-party applications point (*Manage Topology*) | `KIND`, `NAME`, `ENDPOINT`, `INTERNAL_ENDPOINT`, `PARTNER`, `DEPLOYED_APPLICATION` |
| `FLEXFIELD DEPLOYMENT` | Deployment status and error of descriptive (DFF) and extensible (EFF) flexfields — a changed but undeployed flexfield does not show its segments | `FLEXFIELD_TYPE`, `FLEXFIELD_CODE`, `NAME`, `APPLICATION_ID`, `DEPLOYMENT_STATUS`, `DEPLOYMENT_ERROR`, `DEPLOYMENT_DATE`, `LAST_UPDATE_DATE`, `LAST_UPDATED_BY` |

- Enabled rows come first; flexfields with a deployment error first, then those not deployed (`EDITED`: changed since
  the last deployment; `READY`: never deployed).
- `ENDPOINTS` also matches the pattern against the endpoint address.
- Dates are in UTC. At most 20,000 rows per command.
- They read the REST resources `features`, `erpBusinessEvents`, the Topology Manager endpoint resources and
  `descriptiveFlexfields` / `extensibleFlexfields`; a user who may not read one gets Fusion's reason.

To find what differs between two environments, compare the tables (section 7).

## 7. The commands as tables in DUCK queries

Every command is a table function, so a DuckDB query can filter, aggregate and join its result, keep it
(`CREATE TABLE DUCK.x AS SELECT * FROM …`) or compare environments with `host := '…'` (another open connection, see
[Comparing environments](duck_workspace.md#3-mixed-queries-and-the-next-result-page)). A statement calling one in `FROM`
runs in DuckDB without `#d`.

| Function | Command |
|---|---|
| `import_errors([request_id := n] [, interface := 'AP'] [, batch := '…'])` | `IMPORT ERRORS` |
| `ess_requests([job := '…'] [, last := n])`, `ess_request(id)` | `ESS REQUESTS`, `ESS REQUEST` |
| `ess_log(id [, file := 'log' \| 'out' \| 'all'])` | `ESS LOG` |
| `audit_history('table or object' [, days := n] [, from_date := '…'] [, to_date := '…'] [, login := '…'] [, product := '…'])` | `AUDIT` |
| `audit_objects([pattern := '…'])` | `AUDIT OBJECTS` |
| `approval_tasks([state := '…'] [, keyword := '…'] [, since := '…'] [, last := n] [, assignees := true])`, `approval_task(n)` | `APPROVAL TASKS`, `APPROVAL TASK` |
| `features(…)`, `business_events(…)`, `endpoints(…)`, `flexfield_deployment(…)` — each `[pattern := '…'] [, all := true]` | the configuration checks |

```sql
-- the errors of an import, grouped
SELECT document, message, count(*) FROM import_errors(request_id := 1234567) GROUP BY ALL ORDER BY 3 DESC;

-- error lines of a scheduled process and its children
SELECT request_id, file_name, line, text FROM ess_log(1234567) WHERE text ILIKE '%ORA-%' OR text ILIKE '%error%';

-- who changed suppliers' inactive dates this month
SELECT audit_date, user_name, object, old_value, new_value
FROM audit_history('POZ_SUPPLIERS', from_date := '2026-11-01') WHERE column_name = 'END_DATE_ACTIVE';

-- approvers with the most waiting invoices
SELECT assignees, count(*) AS tasks, max(days_waiting) AS oldest
FROM approval_tasks(keyword := 'Invoice', assignees := true) GROUP BY 1 ORDER BY 2 DESC;

-- opt-in features enabled in TEST but not here
SELECT feature_code, feature_name FROM features(host := 'test')
EXCEPT SELECT feature_code, feature_name FROM features();
```

`from`, `to` and `user` are SQL keywords, hence `from_date`, `to_date` and `login`.

## 8. Command summary

| Command | What it does | Section |
|---|---|---|
| `IMPORT ERRORS [id] [FOR interface] [BATCH '…']` | rows an import rejected and why | 2 |
| `ESS REQUESTS ['job'] [LAST n]`, `ESS REQUEST id`, `TIMELINE ESS id` | scheduled process requests | 3 |
| `ESS LOG id [LOG \| OUT \| ALL]` | log and output files, one row per line | 3 |
| `AUDIT [OBJECTS ['pattern']]` | what is audited | 4 |
| `AUDIT table\|object [DAYS n \| FROM '…' [TO '…']] [USER 'login'] [PRODUCT '…']` | who changed what | 4 |
| `APPROVAL TASKS [state] ['word'] [SINCE '…'] [LAST n] [WITH ASSIGNEES]`, `APPROVAL TASK n` | approval tasks | 5 |
| `FEATURES`, `BUSINESS EVENTS`, `ENDPOINTS`, `FLEXFIELD DEPLOYMENT` — `['pattern'] [ALL]` | configuration | 6 |

Related: `USER ACCESS 'user'` (roles and data access), `PROFILE 'pattern'`, `LOOKUP 'type'`, `VALUESET code`,
`ORG STRUCTURE`, `DESCRIBE FLEXFIELD table` — see `HELP` and the [README](../README.md).

## 9. Messages and what to do

| Message (shortened) | Meaning and what to do |
|---|---|
| `IMPORT ERRORS …: no rejected rows in …` | The import had no errors, or its rows were corrected, imported again or purged. Check the request with `ESS REQUEST id` and its log with `ESS LOG id`. |
| `IMPORT ERRORS …: request N is not in the ESS history` | The request was purged or the id is not a request: only interface rows carrying the id were looked for. |
| `IMPORT ERRORS …: X could not be read` | The user may not read that interface; the others were read. |
| `… rows have no batch: give the request id` | That interface keeps no batch id: use `IMPORT ERRORS <id> FOR …`. |
| `ESS LOG …: no files for this request` | Not finished, purged, not a scheduled process, or not visible to the user. |
| `ESS LOG …: the request's files could not be read through the REST resource erpintegrations` | The user lacks access to `erpintegrations`. |
| `AUDIT …: no audited object or table is named …; similar: …` | Use a name from `AUDIT OBJECTS`; audit is turned on in *Manage Audit Policies*. |
| `AUDIT …: no audit events` | Nothing changed in the period, or audit was off for the object then. |
| `AUDIT …: N days asked, at most 366` | Shorten the period. |
| `AUDIT …: no web application … matches …; give it` | Add `PRODUCT '…'` from the `PRODUCT` column of `AUDIT OBJECTS`. |
| `AUDIT …: the audit history could not be read (…)` | The user lacks the privilege to view audit history; Fusion's reason follows. |
| `AUDIT …: the audit service answered FAIL: …` | Fusion refused the request; its reason follows. |
| `AUDIT …: more than 100000 changed attributes` | Shorten the period or give `USER`. |
| `APPROVAL TASKS …: only this user's and its groups' tasks` | The user may not administer workflow tasks; ask for that access to see every user's tasks. |
| `APPROVAL TASK n: the task could not be read` | No such task, or the user may not see it. |
| `…: assignees read for the newest 200 tasks only` | Narrow with a word, `SINCE` or `LAST n`. |
| `FEATURES …: the REST resource features could not be read` (and the other checks) | The user may not read that resource; Fusion's reason follows. |
| `…: a pattern with a quote is not supported` | Leave the quote out (`_` matches any one character). |
| `…: more than 20000 rows in …` | Give a pattern. |
