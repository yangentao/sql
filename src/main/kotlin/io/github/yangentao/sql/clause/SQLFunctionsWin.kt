@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

fun SQLNode.WINDOW(name: String, partionBy: Any?, vararg orderBy: Any): SQLNode {
    this.."WINDOW"..name.."AS ("
    if (partionBy != null) this.."PARTITION BY"..partionBy
    if (orderBy.isNotEmpty()) {
        this.."ORDER BY"
        this..orderBy.toList()
    }
    this..")"
    return this
}

infix fun ExpressFunc.FILTER(condition: Where): ExpressFunc {
    this.."FILTER"
    this.brace(listOf(condition))
    return this.."FILTER (WHERE"..condition..")"
}

fun ExpressFunc.OVER(partionBy: Any?, vararg orderBy: Any): SQLExpress {
    this.."OVER("
    if (partionBy != null) this.."PARTITION BY"..partionBy
    if (orderBy.isNotEmpty()) {
        this.."ORDER BY"
        this..orderBy.toList()
    }
    this..")"
    return this
}

infix fun ExpressFunc.OVER_WIN(winName: String): SQLExpress {
    return this.."OVER("..winName..")"
}

fun ROW_NUMBER(): ExpressFunc {
    return ExpressFunc("ROW_NUMBER");
}

fun RANK(): ExpressFunc {
    return ExpressFunc("RANK");
}

fun DENSE_RANK(): ExpressFunc {
    return ExpressFunc("DENSE_RANK");
}

fun FIRST_VALUE(express: Any): ExpressFunc {
    return ExpressFunc("FIRST_VALUE", listOf(express))
}

fun LAST_VALUE(express: Any): ExpressFunc {
    return ExpressFunc("LAST_VALUE", listOf(express))
}

fun NTH_VALUE(express: Any, n: Int): ExpressFunc {
    return ExpressFunc("NTH_VALUE", listOf(express, n))
}

fun LAG(express: Any, offset: Int?, defaultValue: Any?): ExpressFunc {
    return ExpressFunc("LAG", listOfNotNull(express, offset, defaultValue))
}

fun LEAD(express: Any, offset: Int?, defaultValue: Any?): ExpressFunc {
    return ExpressFunc("LEAD", listOfNotNull(express, offset, defaultValue))
}

fun PERCENT_RANK(): ExpressFunc {
    return ExpressFunc("PERCENT_RANK");
}

fun CUME_DIST(): ExpressFunc {
    return ExpressFunc("CUME_DIST");
}

fun NTILE(n: Int): ExpressFunc {
    return ExpressFunc("NTILE", listOf(n))
}


