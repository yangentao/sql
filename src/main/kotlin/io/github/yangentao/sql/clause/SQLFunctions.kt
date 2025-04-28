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
    return newSQLExp.."DATE_PART("..field..","..source.asKey..")"
}

fun SUM(exp: Any): SQLExpress {
    return newSQLExp.."SUM("..exp.asKey..")"
}

fun MIN(exp: Any): SQLExpress {
    return newSQLExp.."MIN("..exp.asKey..")"
}

fun MAX(exp: Any): SQLExpress {
    return newSQLExp.."MAX("..exp.asKey..")"
}

fun AVG(exp: Any): SQLExpress {
    return newSQLExp.."AVG("..exp.asKey..")"
}

fun TO_NUMBER(exp: Any, format: String): SQLExpress {
    return newSQLExp.."TO_NUMBER("..exp.asKey..", '"..format.."')"
}
