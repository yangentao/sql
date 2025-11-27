@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

open class SQLNode(clause: Any? = null) : SQLExpress(clause)

fun SELECT(vararg exps: Any): SQLNode {
    return _SELECT_LIST(exps.toList())
}

fun SELECT_LIST(exps: List<Any>): SQLNode {
    return _SELECT_LIST(exps)
}

internal fun _SELECT_LIST(exps: List<Any>): SQLNode {
    val node = SQLNode("SELECT")
    if (exps.isEmpty()) return node.."*"
    val first = exps.first()

    if (first is DistinctExp) {
        node..first
        node..exps.slice(1..<exps.size).ifEmpty { "*" }
    } else {
        node..exps
    }
    return node
}

//inline fun <reified T : Any> SQLNode.FROM(): SQLNode {
//    return FROM(T::class)
//}

fun SQLNode.FROM(vararg exps: Any): SQLNode {
    return FROM_LIST(exps.toList())
}

fun SQLNode.FROM_LIST(exps: List<Any>): SQLNode {
    return this.."FROM "..exps
}

private fun SQLNode.WHERE_(condition: Where): SQLNode {
    if (condition.isEmpty) return this
    return this.."WHERE"..condition
}

fun SQLNode.WHERE(vararg conditions: Where): SQLNode {
    return WHERE(conditions.toList())
}

fun SQLNode.WHERE(conditions: List<Where>): SQLNode {
    return WHERE_(AND_ALL(conditions))
}

fun SQLNode.ORDER_BY(vararg exps: Any): SQLNode {
    return this.ORDER_BY_LIST(exps.toList())
}

fun SQLNode.ORDER_BY_LIST(exps: List<Any>): SQLNode {
    if (exps.isEmpty()) return this
    return this.."ORDER BY"..exps
}

fun SQLNode.GROUP_BY(vararg exps: Any): SQLNode {
    return GROUP_BY_LIST(exps.toList())
}

fun SQLNode.GROUP_BY_LIST(exps: List<Any>): SQLNode {
    if (exps.isEmpty()) return this
    return this.."GROUP BY"..exps
}

fun SQLNode.CUBE(vararg exps: Any): SQLNode {
    return this.."CUBE"..exps
}

fun SQLNode.ROLLUP(vararg exps: Any): SQLNode {
    return this.."ROLLUP"..exps
}

fun SQLNode.GROUPING_SETS(vararg exps: List<Any>): SQLNode {
    this.."GROUPING SETS"
    this.addEach(exps.toList(), braced = true) { ls ->
        this.brace(ls)
    }
    return this
}

fun SQLNode.HAVING(condition: Where): SQLNode {
    return this.."HAVING"..condition
}

fun SQLNode.LIMIT(size: Number, offset: Number? = null): SQLNode {
    this.."LIMIT"..size
    if (offset != null) this.."OFFSET"..offset
    return this
}

fun SQLNode.OFFSET(offset: Number): SQLNode {
    return this.."OFFSET"..offset
}

fun SQLNode.LIMIT_OFFSET(size: Number, offset: Number?): SQLNode {
    if (offset == null) return this.."LIMIT"..size
    return this.."LIMIT"..size.."OFFSET"..offset
}

fun SQLNode.RETURNING(columns: List<Any> = emptyList()): SQLNode {
    return if (columns.isEmpty())
        this.."RETURNING *"
    else {
        this.."RETURNING"..columns
    }
}
