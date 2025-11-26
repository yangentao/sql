@file:Suppress("unused")

package io.github.yangentao.sql

import io.github.yangentao.sql.clause.*
import java.sql.Connection
import java.sql.ResultSet

enum class Conflicts {
    Throw, Ignore, Update;
}

@JvmInline
value class InsertIntoValues(private val valueList: ArrayList<List<Any?>>) {
    fun values(vararg values: Any?) {
        valueList += values.toList()
    }
}

fun Connection.insertInto(table: String, vararg cols: Any, block: InsertIntoValues.() -> Unit): InsertResult {
    val valueList: ArrayList<List<Any?>> = ArrayList()
    InsertIntoValues(valueList).apply(block)
    return INSERT_INTO_VALUES(table, cols.toList(), valueList).insert(this)
}

fun Connection.insertInto(table: String, vararg kvs: Pair<String, Any?>): InsertResult {
    return insertInto(table, kvs.toList())
}

fun Connection.insertInto(table: String, kvs: List<Pair<String, Any?>>): InsertResult {
    val node = INSERT_INTO(table, kvs)
    return insert(node.sql, node.arguments)
}

//现有记录和要插入的记录完全一样, 也会返回false, 表示没有更新
fun Connection.upsert(model: TableModel, conflict: Conflicts = Conflicts.Update): InsertResult {
    val pks = model::class.primaryKeysHare
    assert(pks.isNotEmpty())
    val cs = model.propertiesExists
    return this.upsert(model::class.nameSQL, cs.map { it.fieldSQL to model[it] }, pks.map { it.fieldSQL }, conflict = conflict)
}

//upsert
fun Connection.upsert(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update): InsertResult {
    return if (this.isMySQL) {
        upsertMySQL(table, kvs, uniqColumns, conflict = conflict)
    } else if (this.isPostgres || this.isSQLite) {
        upsertPgSQLite(table, kvs, uniqColumns, conflict = conflict)
    } else {
        error("NOT support insert or update!")
    }
}

private fun Connection.upsertPgSQLite(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update): InsertResult {
    val node = buildUpsertPgSQLite(table, kvs, uniqColumns, conflict)
    return this.insert(node.sql, node.arguments)
}

private fun Connection.upsertMySQL(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update): InsertResult {
    val e = SQLNode("INSERT INTO")
    e..table.escapeSQL
    e.parenthesed(kvs.map { it.first.escapeSQL })
    e.."VALUES"
    e.parenthesedAll(kvs.map { it.second }) { e..<it }
    if (conflict == Conflicts.Throw || uniqColumns.isEmpty()) {
        return this.insert(e.sql, e.arguments)
    }

    val updateCols = kvs.filter { it.first !in uniqColumns }
    if (conflict == Conflicts.Ignore || updateCols.isEmpty()) {
        e.buffer.buffer.insert("INSERT".length, " IGNORE")
        return this.insert(e.sql, e.arguments)
    }
    e.."ON DUPLICATE KEY UPDATE"
    e.addEach(updateCols) {
        e..it.first.escapeSQL
        e.."="
        e..<it.second
    }
    return this.insert(e.sql, e.arguments)
}

private fun buildUpsertPgSQLite(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update): SQLNode {
    val e = SQLNode("INSERT INTO")
    e..table.escapeSQL
    e.parenthesed(kvs.map { it.first.escapeSQL })
    e.."VALUES"
    e.parenthesedAll(kvs.map { it.second }) { e..<it }
    if (conflict == Conflicts.Throw || uniqColumns.isEmpty()) {
        return e
    }

    val updateCols = kvs.filter { it.first !in uniqColumns }
    e.."ON CONFLICT"
    e.parenthesedAll(uniqColumns) { e..(it.escapeSQL) }
    if (conflict == Conflicts.Ignore || updateCols.isEmpty()) {
        e.."DO NOTHING"
        return e
    }
    e.."DO UPDATE SET"
    e.addEach(updateCols) {
        e..it.first.escapeSQL
        e.."="
        e..<it.second
    }
    return e
}

fun Connection.upsertModelReturning(model: TableModel, conflict: Conflicts = Conflicts.Update, returning: List<Any> = emptyList()): ResultSet {
    val pks = model::class.primaryKeysHare
    assert(pks.isNotEmpty())
    val cs = model.propertiesExists
    return this.upsertReturning(model::class.nameSQL, cs.map { it.fieldSQL to model[it] }, pks.map { it.fieldSQL }, conflict = conflict, returning = returning)
}

//upsert
fun Connection.upsertReturning(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update, returning: List<Any> = emptyList()): ResultSet {
    if (this.isPostgres || this.isSQLite) {
        return upsertPgSQLiteReturning(table, kvs, uniqColumns, conflict = conflict, returning = returning)
    }
    error("NOT support returning clause")
}

private fun Connection.upsertPgSQLiteReturning(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update, returning: List<Any> = emptyList()): ResultSet {
    val node = buildUpsertPgSQLite(table, kvs, uniqColumns, conflict).RETURNING(returning)
    return this.query(node.sql, node.arguments)
}

