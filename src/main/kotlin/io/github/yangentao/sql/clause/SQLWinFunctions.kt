@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

class WinindowFunction(sql: String? = null) : SQLFunction(sql)

private val newWinFunc: WinindowFunction get() = WinindowFunction()

private fun buildWindowSQL(partionBy: Any?, vararg orderBy: Any): SQLExpress {
    val e = newSQLExp
    if (partionBy != null) e.."PARTITION BY"..partionBy.asKey
    if (orderBy.isNotEmpty()) {
        e.."ORDER BY"
        e.addList(orderBy.toList()) { a, item ->
            a..item.asKey
        }
    }
    return e
}

fun SQLNode.WINDOW(name: String, partionBy: Any?, vararg orderBy: Any): SQLNode {
    return this.."WINDOW"..name.."AS ("..buildWindowSQL(partionBy, *orderBy)..")"
}

fun SQLFunction.OVER(partionBy: Any?, vararg orderBy: Any): SQLExpress {
    return this.."OVER("..buildWindowSQL(partionBy, *orderBy)..")"
}

infix fun SQLFunction.OVER_WIN(winName: String): SQLExpress {
    return this.."OVER("..winName..")"
}

fun ROW_NUMBER(): WinindowFunction {
    return newWinFunc.."ROW_NUMBER()"
}

fun RANK(): WinindowFunction {
    return newWinFunc.."RANK()"
}

fun DENSE_RANK(): WinindowFunction {
    return newWinFunc.."DENSE_RANK()"
}

fun FIRST_VALUE(exp: Any): WinindowFunction {
    return newWinFunc.."FIRST_VALUE("..exp.asKey..")"
}

fun LAST_VALUE(exp: Any): WinindowFunction {
    return newWinFunc.."LAST_VALUE("..exp.asKey..")"
}

//nth > 0
fun NTH_VALUE(exp: Any, nth: Int): WinindowFunction {
    return newWinFunc.."NTH_VALUE("..exp.asKey..","..nth..")"
}

fun LEAD(exp: Any, offset: Int, defaultValue: Any?): WinindowFunction {
    if (defaultValue == null) return newWinFunc.."LEAD("..exp.asKey..","..offset..")"
    return newWinFunc.."LEAD("..exp.asKey..","..offset..","..defaultValue..")"
}

fun LAG(exp: Any, offset: Int, defaultValue: Any?): WinindowFunction {
    if (defaultValue == null) return newWinFunc.."LAG("..exp.asKey..","..offset..")"
    return newWinFunc.."LAG("..exp.asKey..","..offset..","..defaultValue..")"
}