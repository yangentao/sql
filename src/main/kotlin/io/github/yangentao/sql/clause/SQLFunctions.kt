@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

open class SQLFunction(sql: String? = null) : SQLExpress(sql)

private val newFunSQL: SQLFunction get() = SQLFunction()

fun COUNT(exp: Any): SQLFunction {
    return newFunSQL.."COUNT("..exp..")"
}

infix fun SQLFunction.FILTER(condition: Where): Where {
    return Where()..this.."FILTER (WHERE"..condition..")"
}

fun DATE_PART(field: String, source: Any): SQLExpress {
    return newExp.."DATE_PART("..field..","..source.asExpress..")"
}

fun SUM(exp: Any): SQLExpress {
    return newExp.."SUM("..exp.asExpress..")"
}

fun MIN(exp: Any): SQLExpress {
    return newExp.."MIN("..exp.asExpress..")"
}

fun MAX(exp: Any): SQLExpress {
    return newExp.."MAX("..exp.asExpress..")"
}

fun AVG(exp: Any): SQLExpress {
    return newExp.."AVG("..exp.asExpress..")"
}

fun TO_NUMBER(exp: Any, format: String): SQLExpress {
    return newExp.."TO_NUMBER("..exp.asExpress..", '"..format.."')"
}
