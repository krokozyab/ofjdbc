# Secured View Mappings

## Overview

Some Oracle HCM tables hold personal data that Oracle expects to be read through secured views. The driver replaces these table names with their secured views before the query is sent, so that the data security policies Oracle attaches to those views apply. No configuration is needed: write the table name as usual, with or without the `FUSION.` prefix or quotes.

| Table | Read through |
|---|---|
| `HR_ALL_ORGANIZATION_UNITS_F` | `PER_LEGAL_EMPL_SECURED_LIST_V` |
| `HR_ALL_POSITIONS_F` | `PER_POSITION_SECURED_LIST_V` |
| `PER_JOBS_F` | `PER_JOB_SECURED_LIST_V` |
| `PER_LOCATIONS` | `PER_LOCATION_SECURED_LIST_V` |
| `PER_GRADES_F` | `PER_GRADE_SECURED_LIST_V` |
| `PER_ALL_PEOPLE_F` | `PER_PERSON_SECURED_LIST_V` |
| `PER_PERSONS` | `PER_PUB_PERS_SECURED_LIST_V` |
| `PER_LEGISLATIVE_DATA_GROUPS` | `PER_LDG_SECURED_LIST_V` |
| `PAY_ALL_PAYROLLS_F` | `PAY_PAYROLL_SECURED_LIST_V` |
| `CMP_SALARY` | `CMP_SALARY_SECURED_LIST_V` |
| `PER_ALL_ASSIGNMENTS_M` | `PER_ASSIGNMENT_SECURED_LIST_V` |
| `HR_DOCUMENTS_OF_RECORD` | `HR_DOR_SECURED_LIST_V` |
| `HR_DOCUMENTS_OF_RECORD_ARCHIVE` | `HR_DOR_ARCHIVE_SECURED_LIST_V` |

Any other table is left unchanged. Names inside string literals and comments are not replaced, so a query such as `SELECT column_name FROM all_tab_columns WHERE table_name = 'PER_ALL_PEOPLE_F'` asks about the table itself.

## Missing Columns

Most secured views lack some columns of their table. A query naming such a column fails with `ORA-00904: invalid identifier`; the driver adds a line to the error naming the secured views the query was read through. Examples of columns the views do not have:

| Table | Columns missing in the secured view (examples) |
|---|---|
| `CMP_SALARY` | `ANNUAL_SALARY`, `ANNUAL_FT_SALARY`, `CURRENCY_CODE`, `SALARY_BASIS_CODE`, `COMPA_RATIO`, `GRADE_ID`, `JOB_ID`, `FTE_VALUE` (75 of 162 columns) |
| `HR_ALL_POSITIONS_F` | `COST_CENTER`, `BUDGET_AMOUNT`, `BUDGETED_POSITION_FLAG`, `STANDARD_WORKING_HOURS` |
| `PER_JOBS_F` | `JOB_SUB_FAMILY`, `SCHEDULING_GROUP`, `CATEGORY_CODE`, `GRADE_LADDER_ID` |
| `PER_ALL_ASSIGNMENTS_M` | `TERMINATION_DATE`, `LAST_WORKING_DATE`, `NOTIFICATION_DATE`, `SOURCE_ASSIGNMENT_ID` |
| `PAY_ALL_PAYROLLS_F` | `REPORTING_NAME`, `ENTERPRISE_ID` |
| `PER_LOCATIONS` | `CATEGORY_CODE`, `COUNTRY`, `EMPLOYEE_LOCATION_FLAG` |
| `HR_ALL_ORGANIZATION_UNITS_F` | `ORGANIZATION_CODE`, `SEED_DATA_SOURCE` |
| `PER_GRADES_F`, `PER_PERSONS` | `CATEGORY_CODE` |

`PER_ALL_PEOPLE_F`, `PER_LEGISLATIVE_DATA_GROUPS` and the documents of record tables have all their columns. Database metadata (DBeaver's column list) shows the columns of the table, so autocompletion may offer a column the view does not have.

## Limitations

- The replacement is applied to the query text. It is a convenience, not a security boundary: SQL passed as a string to a function (for example `dbms_xmlgen.getxml('select … from per_all_people_f')`) is not rewritten.
- `HR_ALL_ORGANIZATION_UNITS_F` is read through the legal employer view. `PER_DEPARTMENT_SECURED_LIST_V` is another secured view of the same table; query it by name when you need it.
- Some tables have no single secured view and are left unchanged: `HRT_PROFILE_ITEMS` (one view per profile section) and `HRG_GOALS` (separate views for performance and development goals). Query the view you need by name.
