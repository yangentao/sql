@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.escapeSQL
import io.github.yangentao.sql.unescapeSQL

fun CREATE_INDEX(tableName: String, columnName: String): String {
    val idxName = "${tableName.unescapeSQL}_${columnName.unescapeSQL}_INDEX"
    return "CREATE INDEX  $idxName ON ${tableName.escapeSQL}(${columnName.escapeSQL})"
}

fun CREATE_TABLE(tableName: String, columns: List<String>, options: List<String> = emptyList()): String {
    return buildString {
        append("CREATE TABLE IF NOT EXISTS ${tableName.escapeSQL} (")
        append(columns.joinToString(", "))
        append(")")
        append(options.joinToString(", "))
    }
}

fun DROP_TABLE(tableName: String): String {
    return "DROP TABLE IF EXISTS ${tableName.escapeSQL}"
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
    e.brace(cols.map { ShortExpress(it) })
    e..VALUES_LIST(values)
    return e
}

fun DELETE_FROM(table: Any): SQLNode {
    return SQLNode("DELETE FROM")..table
}

// DELETE FROM t1 USING t2 WHERE t1.id = t2.userId AND ....
fun SQLNode.USING(express: Any): SQLNode {
    return this.."USING"..express
}
// UPDATE table SET ... FROM table2 WHERE
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

val SQLNode.FOR_UPDATE: SQLNode get() = this.."FOR UPDATE"

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

fun VALUES(vararg values: List<Any?>): SQLNode {
    return VALUES_LIST(values.toList())
}

fun VALUES_LIST(values: List<List<Any?>>): SQLNode {
    val e = SQLNode("VALUES")
    e.addEach(values, ",") { vs ->
        e.addEach(vs, braced = true) { v ->
            e..<v
        }
    }
    return e
}
