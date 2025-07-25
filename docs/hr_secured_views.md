# Secured View Mappings

## Overview

Some Oracle HR tables contain sensitive information and must only be accessed through their secured views. Driver automatically substitutes these tables with their secured equivalents before the SQL is sent to the WSDL service. The mapping between table names and secured view names is stored in `SecuredViewMappings.kt`.

## Mapping Details

The `SecuredViewMappings` object contains a `tableToView` map. When `sendSqlViaWsdl` normalizes the SQL query it calls `SecuredViewMappings.apply()` which replaces any matching table names with their secured view.

```kotlin
object SecuredViewMappings {
    val tableToView = mapOf(
        "HR_ALL_ORGANIZATION_UNITS_F" to "PER_DEPARTMENT_SECURED_LIST_V",
        "HR_ALL_POSITIONS_F" to "PER_POSITION_SECURED_LIST_V",
        "PER_JOBS_F" to "PER_JOB_SECURED_LIST_V",
        "PER_LOCATIONS" to "PER_LOCATION_SECURED_LIST_V",
        "PER_GRADES_F" to "PER_GRADE_SECURED_LIST_V",
        "PER_ALL_PEOPLE_F" to "PER_PERSON_SECURED_LIST_V",
        "PER_PERSONS" to "PER_PUB_PERS_SECURED_LIST_V",
        "HR_ALL_ORGANIZATION_UNITS_F" to "PER_LEGAL_EMPL_SECURED_LIST_V",
        "PER_LEGISLATIVE_DATA_GROUPS" to "PER_LDG_SECURED_LIST_V",
        "PAY_ALL_PAYROLLS_F" to "PAY_PAYROLL_SECURED_LIST_V",
        "CMP_SALARY" to "CMP_SALARY_SECURED_LIST_V",
        "PER_ALL_ASSIGNMENTS_M" to "PER_ASSIGNMENT_SECURED_LIST_V"
    )

    fun apply(sql: String): String {
        var result = sql
        for ((table, view) in tableToView) {
            val regex = Regex("\\b" + Regex.escape(table) + "\\b", RegexOption.IGNORE_CASE)
            result = regex.replace(result) { view }
        }
        return result
    }
}
```

Any table not listed in the map is left unchanged.

## Usage

No configuration is required. Simply write queries normally. If they reference the protected HR tables listed above, they will automatically be rewritten to use the secured views.