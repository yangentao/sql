@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

fun SQLNode.INSERT_INTO(table: Any, vararg keyValues: Pair<Any, Any?>): SQLNode {
    return this..INSERT_INTO(table, keyValues.toList())
}

fun INSERT_INTO(table: Any, vararg keyValues: Pair<Any, Any?>): SQLNode {
    return INSERT_INTO(table, keyValues.toList())
}

fun INSERT_INTO(table: Any, keyValues: List<Pair<Any, Any?>>): SQLNode {
    return INSERT_INTO_VALUES(table, keyValues.map { it.first }, listOf(keyValues.map { it.second }))
}

fun INSERT_INTO_VALUES(table: Any, cols: List<Any>, values: List<List<Any?>>): SQLNode {
    val e = SQLNode()
    e.."INSERT INTO"..table.."("
    e.addList(cols, ",") { ex, p ->
        ex..p.asColumn
    }
    e..") VALUES "

    for ((idx, vs) in values.withIndex()) {
        e.."("
        e.addList(vs, ",") { ex, p ->
            when (p) {
                null -> ex.."NULL"
                is Number -> ex..p.toString()
                else -> ex.."?" argValue p
            }
        }
        if (idx == values.lastIndex) {
            e..")"
        } else {
            e.."), "
        }
    }
    return e
}

fun SQLNode.DELETE_FROM(table: Any): SQLNode {
    return this.._DELETE_FROM(table)
}

fun DELETE_FROM(table: Any): SQLNode {
    return _DELETE_FROM(table)
}

private fun _DELETE_FROM(table: Any): SQLNode {
    return SQLNode("DELETE FROM")..table
}

fun SQLNode.UPDATE(table: Any): SQLNode {
    return this.._UPDATE(table)
}

fun UPDATE(table: Any): SQLNode {
    return _UPDATE(table)
}

private fun _UPDATE(table: Any): SQLNode {
    return SQLNode("UPDATE")..table
}

fun SQLNode.SET(vararg keyValues: Pair<Any, Any?>): SQLNode {
    return this.SET(keyValues.toList())
}

fun SQLNode.SET(keyValues: List<Pair<Any, Any?>>): SQLNode {
    this.."SET"
    this.addList(keyValues) { e, p ->
        val col = p.first.asColumn
        val value = p.second
        when (value) {
            null -> e..col.."= NULL"
            is Number -> e..col.."="..value
            else -> e..col.."= ?" argValue value
        }
    }
    return this
}
