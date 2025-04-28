package io.github.yangentao.sql

import io.github.yangentao.reflect.plusAssign
import io.github.yangentao.sql.clause.INSERT_INTO_VALUES
import io.github.yangentao.sql.clause.insert
import java.sql.Connection

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
    val sql = "INSERT INTO ${table.escapeSQL} (${kvs.joinToString(", ") { it.first.escapeSQL }} ) VALUES ( ${kvs.joinToString(", ") { "? " }} ) "
    return insert(sql, kvs.map { it.second })
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
    val buf = StringBuilder(512)
    buf.append("INSERT INTO ${table.escapeSQL} (${kvs.joinToString(", ") { it.first.escapeSQL }} ) VALUES ( ${kvs.joinToString(", ") { "? " }} ) ")
    if (conflict == Conflicts.Throw || uniqColumns.isEmpty()) {
        return this.insert(buf.toString(), kvs.map { it.second })
    }

    val updateCols = kvs.filter { it.first !in uniqColumns }
    buf += " ON CONFLICT(${uniqColumns.joinToString(",") { it.escapeSQL }})"
    if (conflict == Conflicts.Ignore || updateCols.isEmpty()) {
        buf += " DO NOTHING "
        return this.insert(buf.toString(), kvs.map { it.second })
    }

    buf += " DO UPDATE SET  "
    buf += updateCols.joinToString(", ") { "${it.first.escapeSQL} = ? " }
    return this.insert(buf.toString(), kvs.map { it.second } + updateCols.map { it.second })
}

private fun Connection.upsertMySQL(table: String, kvs: List<Pair<String, Any?>>, uniqColumns: List<String>, conflict: Conflicts = Conflicts.Update): InsertResult {
    val buf = StringBuilder(512)
    if (conflict == Conflicts.Throw || uniqColumns.isEmpty()) {
        buf.append("INSERT INTO ${table.escapeSQL} (${kvs.joinToString(", ") { it.first.escapeSQL }} ) VALUES ( ${kvs.joinToString(", ") { "? " }} ) ")
        return this.insert(buf.toString(), kvs.map { it.second })
    }

    val updateCols = kvs.filter { it.first !in uniqColumns }
    if (conflict == Conflicts.Ignore || updateCols.isEmpty()) {
        buf.append("INSERT IGNORE INTO ${table.escapeSQL} (${kvs.joinToString(", ") { it.first.escapeSQL }} ) VALUES ( ${kvs.joinToString(", ") { "? " }} ) ")
        return this.insert(buf.toString(), kvs.map { it.second })
    }

    buf.append("INSERT INTO ${table.escapeSQL} (${kvs.joinToString(", ") { it.first.escapeSQL }} ) VALUES ( ${kvs.joinToString(", ") { "? " }} ) ")
    buf += " ON DUPLICATE KEY UPDATE "
    buf += updateCols.joinToString(", ") { "${it.first.escapeSQL} = ? " }
    return this.insert(buf.toString(), kvs.map { it.second } + updateCols.map { it.second })
}

