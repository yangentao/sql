@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

class ExpressFunc(express: Any, args: List<Any> = emptyList()) : SQLExpress(express) {
    init {
        this.brace(args)
    }
}

fun COALESCE(vararg values: Any): SQLExpress {
    return ExpressFunc("COALESCE", listOf(values))
}

fun AVG(exp: Any): ExpressFunc {
    return ExpressFunc("AVG", listOf(exp))
}

fun SUM(exp: Any): ExpressFunc {
    return ExpressFunc("SUM", listOf(exp))
}

fun COUNT(exp: Any): ExpressFunc {
    return ExpressFunc("COUNT", listOf(exp))
}

fun TOTAL(exp: Any): ExpressFunc {
    return ExpressFunc("TOTAL", listOf(exp))
}

fun MIN(exp: Any): ExpressFunc {
    return ExpressFunc("MIN", listOf(exp))
}

fun MAX(exp: Any): ExpressFunc {
    return ExpressFunc("MAX", listOf(exp))
}

fun MEDIAN(express: Any): ExpressFunc {
    return ExpressFunc("MEDIAN", listOf(express))
}

fun GROUP_CONCAT(express: Any, sep: String = "','"): ExpressFunc {
    return ExpressFunc("GROUP_CONCAT", listOf(express, sep))
}

fun STRING_AGG(express: Any, sep: String = "','"): ExpressFunc {
    return ExpressFunc("STRING_AGG", listOf(express, sep))
}

fun PERCENTILE(express: Any, p: Double): ExpressFunc {
    return ExpressFunc("PERCENTILE", listOf(express, p))
}

fun PERCENTILE_CONT(express: Any, p: Double): ExpressFunc {
    return ExpressFunc("PERCENTILE_CONT", listOf(express, p))
}

fun PERCENTILE_DISC(express: Any, p: Double): ExpressFunc {
    return ExpressFunc("PERCENTILE_DISC", listOf(express, p))
}

fun DATE_PART(field: String, source: Any): ExpressFunc {
    return ExpressFunc("DATE_PART", listOf(field, source))
}

fun TO_NUMBER(exp: Any, format: String): ExpressFunc {
    return ExpressFunc("TO_NUMBER", listOf(exp, format))
}

fun LEAST(vararg exps: Any): SQLExpress {
    return SQLExpress("LEAST").brace(exps)
}

fun GREATEST(vararg exps: Any): SQLExpress {
    return SQLExpress("GREATEST").brace(exps)
}
