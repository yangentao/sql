@file:Suppress("unused")

package io.github.yangentao.sql

import io.github.yangentao.reflect.decodeValue
import io.github.yangentao.anno.userName
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters

//FIXME ResultSetProxy 修改了默认的ResultSet.close()行为， 会直接关闭Statement, 以便Connection进入可复用状态

val ResultSet.currentRow: ResultRow get() = ResultRow(this)

fun ResultSet.exists(): Boolean = this.use { this.next() }

fun ResultSet.oneInt(index: Int = 1): Int? {
    return this.one { intValue(index) }
}

fun ResultSet.oneLong(index: Int = 1): Long? {
    return this.one { longValue(index) }
}

inline fun <reified T : BaseModel> ResultSet.listOrm(): List<T> {
    return this.list { orm() }
}

fun <T : BaseModel> ResultSet.listOrm(cls: KClass<T>): List<T> {
    return this.list { orm(cls) }
}

inline fun <reified T : BaseModel> ResultSet.oneOrm(): T? {
    return this.one { orm() }
}

fun <T : BaseModel> ResultSet.oneOrm(cls: KClass<T>): T? {
    return this.one { orm(cls) }
}

inline fun <reified T : Any> ResultSet.listModel(): List<T> {
    return this.list { model() }
}

fun <T : Any> ResultSet.listModel(cls: KClass<T>): List<T> {
    return this.list { model(cls) }
}

inline fun <reified T : Any> ResultSet.oneModel(): T? {
    return this.one { model() }
}

fun <T : Any> ResultSet.oneModel(cls: KClass<T>): T? {
    return this.one { model(cls) }
}

inline fun <reified T : Any> ResultSet.oneDataClass(): T? {
    return this.one { dataClassByIndex() }
}

fun <T : Any> ResultSet.oneDataClass(cls: KClass<T>): T? {
    return this.one { dataClassByIndex(cls) }
}

inline fun <reified T : Any> ResultSet.oneDataClassByName(): T? {
    return this.one { dataClassByName() }
}

fun <T : Any> ResultSet.oneDataClassByName(cls: KClass<T>): T? {
    return this.one { dataClassByName(cls) }
}

//data class, user primary constructor!  by index
inline fun <reified T : Any> ResultSet.listDataClass(): List<T> {
    val func = T::class.primaryConstructor!!
    val params = func.valueParameters
    val map = LinkedHashMap<KParameter, Any?>()

    return this.list {
        map.clear()
        for ((i, p) in params.withIndex()) {
            if (i + 1 > resultSet.metaData.columnCount) break
            val v = objectValue(i + 1)
            map[p] = p.decodeValue(v)
        }
        func.callBy(map)
    }
}

//data class, user primary constructor!  by parameter name
inline fun <reified T : Any> ResultSet.listDataClassbyName(): List<T> {
    val func = T::class.primaryConstructor!!
    val params = func.valueParameters
    val map = LinkedHashMap<KParameter, Any?>()
    return this.list<T> {
        map.clear()
        for (p in params) {
            val v = resultSet.getObject(p.userName)
            map[p] = p.decodeValue(v)
        }
        func.callBy(map)
    }
}

fun <T> ResultSet.one(block: ResultRow.() -> T?): T? {
    return this.use {
        if (it.next()) {
            ResultRow(it).block()
        } else null
    }
}

fun <T> ResultSet.list(block: ResultRow.() -> T?): List<T> {
    val ls = ArrayList<T>(128)
    this.use {
        while (it.next()) {
            ResultRow(it).block()?.also { ls.add(it) }
        }
    }
    return ls
}

fun ResultSet.each(block: (ResultRow) -> Unit) {
    return this.use {
        while (it.next()) {
            block(ResultRow(it))
        }
    }
}

fun <T : Any> ResultSet.map(block: (ResultRow) -> T?): List<T> {
    return this.list { block(this) }
}

fun ResultSet.dump() {
    val meta = this.metaData
    val sb = StringBuilder(512)
    each {
        sb.setLength(0)
        for (i in meta.indices) {
            val label = meta.labelAt(i)
            val value = it.objectValue(i)
            sb.append(label).append("=").append(value).append(", ")
        }
        println(sb.toString())
    }
}

fun ResultSet.dumpText(): String {
    val meta = this.metaData
    val sb = StringBuilder(1024)
    this.each {
        for (i in meta.indices) {
            val label = meta.labelAt(i)
            val value = it.objectValue(i)
            sb.append(label).append("=").append(value).append(", ")
        }
        sb.appendLine()
    }
    return sb.toString()
}

operator fun ResultSet.iterator(): Iterator<ResultSet> {
    return ResultSetIterator(this)
}

class ResultSetIterator(private val rs: ResultSet) : Iterator<ResultSet> {
    override operator fun next(): ResultSet {
        return rs
    }

    override operator fun hasNext(): Boolean {
        val b = rs.next()
        if (!b) rs.close()
        return b
    }
}

val ResultSetMetaData.indices: IntRange get() = 1..this.columnCount
fun ResultSetMetaData.labelAt(index: Int): String {
    return getColumnLabel(index).unescapeSQL
}