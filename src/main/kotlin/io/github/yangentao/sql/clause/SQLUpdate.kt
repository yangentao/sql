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
    val e = SQLNode("INSERT INTO")
    e..table
    e.parenthesed(cols.map { ShortExpress(it) })
    e.."VALUES"
    e.addEach(values, ",") { vs ->
        e.."("
        e.addEachX(vs) { v ->
            if (v == null) e.."NULL" else e..<v
        }
        e..")"
    }
    return e
}

fun SQLNode.DELETE_FROM(table: Any): SQLNode {
    return this.."DELETE FROM"..table
}

fun DELETE_FROM(table: Any): SQLNode {
    return SQLNode("DELETE FROM")..table
}

fun SQLNode.UPDATE(table: Any): SQLNode {
    return this.."UPDATE"..table
}

fun UPDATE(table: Any): SQLNode {
    return SQLNode("UPDATE")..table
}

fun SQLNode.SET(vararg keyValues: Pair<Any, Any?>): SQLNode {
    return this.SET(keyValues.toList())
}

fun SQLNode.SET(keyValues: List<Pair<Any, Any?>>): SQLNode {
    this.."SET"
    this.addEach(keyValues) { item ->
        this..ShortExpress(item.first)
        this.."="
        if (item.second == null) this.."NULL" else this..<(item.second!!)
    }
    return this
}

infix fun PropSQL.INC_INT(n: Int): Pair<PropSQL, SQLExpress> {
    val s: String = if (n < 0) n.toString() else "+$n"
    return this.SELF_OP(s)
}

infix fun PropSQL.INC_LONG(n: Long): Pair<PropSQL, SQLExpress> {
    val s: String = if (n < 0) n.toString() else "+$n"
    return this.SELF_OP(s)
}

infix fun PropSQL.INC_REAL(n: Double): Pair<PropSQL, SQLExpress> {
    val s: String = if (n < 0) n.toString() else "+$n"
    return this.SELF_OP(s)
}

infix fun PropSQL.SELF_OP(s: String): Pair<PropSQL, SQLExpress> {
    val e = SQLExpress(this)..s
    return this to e
}