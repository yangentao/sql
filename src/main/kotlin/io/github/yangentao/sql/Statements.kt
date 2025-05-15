@file:Suppress("SqlNoDataSourceInspection", "ConstPropertyName", "unused")

package io.github.yangentao.sql

import io.github.yangentao.kson.*
import io.github.yangentao.types.Prop
import io.github.yangentao.types.plusAssign
import io.github.yangentao.types.rootError
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

/**
 * Created by entaoyang@163.com on 2017/6/10.
 */
typealias ArgList = List<Any?>

fun Connection.insert(sql: String, arguments: ArgList): InsertResult {
    try {
        this.prepare(sql, arguments, true).use {
            val n = it.executeUpdate()
            return if (n > 0) {
                InsertResult(n, it.returnKeys())
            } else {
                InsertResult(n, emptyList())
            }
        }
    } catch (ex: Throwable) {
        SQLog.err("SQL Error: ", sql)
        SQLog.err("SQL Error Args: ", arguments)
        SQLog.err("Error Message: ", ex.rootError.message)
        throw ex
    }
}

fun Connection.query(sql: String, arguments: ArgList = emptyList()): ResultSet {
    try {
        return this.prepare(sql, arguments).executeQuery()
    } catch (ex: Throwable) {
        SQLog.err("SQL Error: ", sql)
        SQLog.err("Args: ", arguments)
        SQLog.err("Error Message: ", ex.rootError.message)
        throw ex
    }
}

fun Connection.update(sql: String, arguments: ArgList = emptyList()): Int {
    try {
        return this.prepare(sql, arguments).use {
            it.executeUpdate()
        }
    } catch (ex: Throwable) {
        SQLog.err("SQL Error: ", sql)
        SQLog.err("Args: ", arguments)
        SQLog.err("Error Message: ", ex.rootError.message)
        throw ex
    }
}

fun Connection.exec(sql: String, arguments: ArgList = emptyList()): Boolean {
    try {
        return this.prepare(sql, arguments).use {
            it.execute()
        }
    } catch (ex: Exception) {
        SQLog.err("Error SQL: ", sql)
        SQLog.err("Args: ", arguments)
        SQLog.err("Error Message: ", ex.rootError.message)
        throw ex
    }
}

inline fun Connection.trans(block: (Connection) -> Unit) {
    try {
        this.autoCommit = false
        block(this)
        this.commit()
    } catch (ex: Exception) {
        this.rollback()
        ex.printStackTrace()
        SQLog.err(ex)
        SQLog.err(ex.stackTraceToString())
        throw ex
    } finally {
        this.autoCommit = true
    }
}

@Suppress("UNCHECKED_CAST")
fun PreparedStatement.setParams(params: ArgList): PreparedStatement {
    val isMySQL = this.connection.isMySQL
    for ((i, v) in params.withIndex()) {
        if (v == null || v is KsonNull) {
            this.setObject(i + 1, null)
            continue
        }
        if (isMySQL) {
            val vv: Any = when (v) {
                is KsonObject -> v.toString()
                is KsonArray -> v.toString()
                is KsonString -> v.data
                is KsonNum -> v.data
                is KsonBool -> v.data
                is KsonBlob -> v.data
                is CharArray -> v.joinToString(",")
                is BooleanArray -> v.joinToString(",")
                is ShortArray -> v.joinToString(",")
                is IntArray -> v.joinToString(",")
                is LongArray -> v.joinToString(",")
                is FloatArray -> v.joinToString(",")
                is DoubleArray -> v.joinToString(",")
                else -> when (v::class) {
                    Array<Boolean>::class -> (v as Array<Boolean>).joinToString(",")
                    Array<Char>::class -> (v as Array<Char>).joinToString(",")
                    Array<Byte>::class -> (v as Array<Byte>).joinToString(",")
                    Array<Short>::class -> (v as Array<Short>).joinToString(",")
                    Array<Int>::class -> (v as Array<Int>).joinToString(",")
                    Array<Long>::class -> (v as Array<Long>).joinToString(",")
                    Array<Float>::class -> (v as Array<Float>).joinToString(",")
                    Array<Double>::class -> (v as Array<Double>).joinToString(",")
                    Array<String>::class -> (v as Array<String>).joinToString(",")
                    else -> v
                }

            }
            this.setObject(i + 1, vv)
            continue
        }
        val vv: Any = when (v) {
            is KsonObject -> v.toString()
            is KsonArray -> v.toString()
            is KsonString -> v.data
            is KsonNum -> v.data
            is KsonBool -> v.data
            is KsonBlob -> v.data
            else -> v
        }
        this.setObject(i + 1, vv)
    }
    return this
}

val Connection.isPostgres: Boolean get() = this.metaData.databaseProductName == "PostgreSQL"
val Connection.isMySQL: Boolean get() = this.metaData.databaseProductName == "MySQL"
val Connection.isSQLite: Boolean get() = this.metaData.databaseProductName == "SQLite"

private fun postJsonSQL(sql: String, args: ArgList): String {
    if (!args.any { it is KsonObject || it is KsonArray }) {
        return sql
    }
    val jsonIndexs = ArrayList<Int>()
    for ((i, item) in args.withIndex()) {
        if (item is KsonObject || item is KsonArray) {
            jsonIndexs += i
        }
    }
    if (jsonIndexs.isEmpty()) return sql
    val sb = StringBuilder(sql.length + 12)
    var idx = -1
    for (i in sql.indices) {
        val ch: Char = sql[i]
        sb += ch
        if (ch == '?') {
            idx += 1
            if (jsonIndexs.contains(idx)) {
                if (i < sql.length - 2 && sql[i + 1] != ':' && sql[i + 2] != ':') {
                    sb += "::json"
                }
            }
        }
    }
    return sb.toString()
}

@Suppress("SqlSourceToSinkFlow")
fun Connection.prepare(sql: String, args: ArgList, genKeys: Boolean = false): PreparedStatement {
    val newSQL = if (isPostgres) postJsonSQL(sql, args) else sql
    SQLog.debug("SQL: ", newSQL)
    if (args.isNotEmpty()) SQLog.debug("     ", args)
    if (genKeys) {
        return this.prepareStatement(newSQL, Statement.RETURN_GENERATED_KEYS).setParams(args)
    }
    return this.prepareStatement(newSQL).setParams(args)
}

//index from 1
data class ReturnKey(val index: Int, val label: String, val key: Long)
data class InsertResult(val count: Int, val returnkeys: List<ReturnKey>) {
    val success: Boolean get() = count > 0
    val key: Long get() = returnkeys.firstOrNull()?.key ?: 0L
    val firstKey: Long? get() = returnkeys.firstOrNull()?.key
    val secondKey: Long? get() = returnkeys.getOrNull(1)?.key
    fun keyOf(label: String): Long? {
        return returnkeys.firstOrNull { it.label.equals(label, true) }?.key
    }

    fun keyOf(prop: Prop): Long? {
        return returnkeys.firstOrNull { it.label.equals(prop.fieldSQL, true) }?.key
    }
}

fun PreparedStatement.returnKeys(): List<ReturnKey> {
    val rs = this.generatedKeys
    val meta = rs.metaData
    val ls = ArrayList<ReturnKey>()
    use {
        while (rs.next()) {
            for (i in meta.indices) {
                if (meta.isAutoIncrement(i)) {
                    ls += ReturnKey(index = i, label = meta.labelAt(i), key = rs.getLong(i))
                }
            }
        }
        return ls
    }
}