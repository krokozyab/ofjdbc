package my.jdbc.wsdl_driver

object SecuredViewMappings {
    val tableToView: Map<String, String> = listOf(
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
    ).toMap()

    fun apply(sql: String): String {
        var result = sql
        for ((table, view) in tableToView) {
            val regex = Regex("\\b" + Regex.escape(table) + "\\b", RegexOption.IGNORE_CASE)
            result = regex.replace(result) { view }
        }
        return result
    }
}