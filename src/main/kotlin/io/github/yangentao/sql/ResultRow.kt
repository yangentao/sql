@file:Suppress("unused", "UNCHECKED_CAST")

package io.github.yangentao.sql

import io.github.yangentao.anno.Exclude
import io.github.yangentao.anno.userName
import io.github.yangentao.kson.KsonArray
import io.github.yangentao.kson.KsonObject
import io.github.yangentao.types.decodeValue
import io.github.yangentao.types.filterTyped
import io.github.yangentao.types.ieq
import io.github.yangentao.types.isPublic
import io.github.yangentao.types.setPropValue
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters

@JvmInline
value class ResultRow(val resultSet: ResultSet) {
    val metaData: ResultSetMetaData get() = resultSet.metaData

    fun stringValue(index: Int = 1): String? = resultSet.getString(index)
    fun stringValue(label: String): String? = resultSet.getString(label)

    fun intValue(index: Int = 1): Int? = resultSet.getInt(index)
    fun intValue(label: String): Int? = resultSet.getInt(label)

    fun longValue(index: Int = 1): Long? = resultSet.getLong(index)
    fun longValue(label: String): Long? = resultSet.getLong(label)

    fun doubleValue(index: Int = 1): Double? = resultSet.getDouble(index)
    fun doubleValue(label: String): Double? = resultSet.getDouble(label)

    fun boolValue(index: Int = 1): Boolean? = resultSet.getBoolean(index)
    fun boolValue(label: String): Boolean? = resultSet.getBoolean(label)

    fun anyValue(index: Int = 1): Any? = resultSet.getObject(index)
    fun anyValue(label: String): Any? = resultSet.getObject(label)

    fun objectValue(index: Int = 1): Any? = resultSet.getObject(index)
    fun objectValue(label: String): Any? = resultSet.getObject(label)

    fun map(): LinkedHashMap<String, Any?> {
        val meta: ResultSetMetaData = this.resultSet.metaData
        val map = LinkedHashMap<String, Any?>()
        for (i in meta.indices) {
            if (meta.getColumnTypeName(i) == "json") {
                map[meta.labelAt(i)] = resultSet.getString(i)
            } else {
                map[meta.labelAt(i)] = resultSet.getObject(i)
            }
        }
        return map
    }

    fun jsonObject(): KsonObject {
        val meta: ResultSetMetaData = this.resultSet.metaData
        val yo = KsonObject(meta.columnCount + 2)
        for (i in meta.indices) {
            val typeName = meta.getColumnTypeName(i)
            val value: Any? = if (typeName in jsonTypes) {
                val js = resultSet.getString(i)?.trim()
                if (js.isNullOrEmpty()) {
                    null
                } else if (js.startsWith("{")) {
                    KsonObject(js)
                } else if (js.startsWith("[")) {
                    KsonArray(js)
                } else {
                    null
                }
            } else {
                resultSet.getObject(i)
            }
            yo.putAny(meta.labelAt(i), value)
        }
        return yo
    }

    inline fun <reified T : Any> model(): T {
        return this.model(T::class, null)
    }

    fun <T : Any> model(cls: KClass<T>, properties: List<KMutableProperty<*>>? = null): T {
        val meta: ResultSetMetaData = this.resultSet.metaData
        val m: T = cls.createInstance()
        val propList: List<KMutableProperty<*>> = if (properties.isNullOrEmpty()) {
            cls.memberProperties.filterTyped { it.isPublic && !it.hasAnnotation<Exclude>() }
        } else {
            properties
        }
        for (i in meta.indices) {
            val label = meta.labelAt(i)
            val prop = propList.firstOrNull { it.userName ieq label } ?: continue
            val pvalue: Any? = prop.decodeValue(resultSet.getObject(i))
            if (!prop.returnType.isMarkedNullable && pvalue == null) {
                continue
            }
            prop.setPropValue(m, pvalue)
        }
        return m
    }

    inline fun <reified T : BaseModel> orm(): T {
        return orm(T::class)
    }

    fun <T : BaseModel> orm(cls: KClass<T>): T {
        val map = this.map()
        val inst = cls.createInstance()
        inst.model.putAll(map)
        return inst
    }

    //data class, user primary constructor!  by index
    inline fun <reified T : Any> dataClassByIndex(): T {
        return dataClassByIndex(T::class)
    }

    fun <T : Any> dataClassByIndex(cls: KClass<T>): T {
        val func = cls.primaryConstructor!!
        val params = func.valueParameters
        val map = LinkedHashMap<KParameter, Any?>()
        for ((i, p) in params.withIndex()) {
            if (i + 1 > resultSet.metaData.columnCount) break
            val v = resultSet.getObject(i + 1)
            map[p] = p.decodeValue(v)
        }
        return func.callBy(map)
    }

    inline fun <reified T : Any> dataClassByName(): T {
        return dataClassByName(T::class)
    }

    fun <T : Any> dataClassByName(cls: KClass<T>): T {
        val func = cls.primaryConstructor!!
        val params = func.valueParameters
        val map = LinkedHashMap<KParameter, Any?>()
        for (p in params) {
            val v = resultSet.getObject(p.userName)
            map[p] = p.decodeValue(v)
        }
        return func.callBy(map)
    }

    fun <R> valueAt(prop: KProperty<R>, index: Int = 1): R? {
        return prop.decodeValue(resultSet.getObject(index)) as? R

    }

    inline fun <reified T : Any> valueAt(index: Int = 1): T? {
        return valueAt(T::class, index)
    }

    fun <T : Any> valueAt(cls: KClass<*>, index: Int = 1): T? {
        return cls.decodeValue(resultSet.getObject(index)) as? T
    }

    fun <R> valueBy(prop: KProperty<R>): R? {
        return prop.decodeValue(resultSet.getObject(prop.userName)) as? R

    }

    fun <R> valueBy(prop: KProperty<R>, label: String): R? {
        return prop.decodeValue(resultSet.getObject(label)) as? R

    }

    inline fun <reified T : Any> valueBy(label: String): T? {
        return valueBy(T::class, label)
    }

    fun <T : Any> valueBy(cls: KClass<*>, label: String): T? {
        return cls.decodeValue(resultSet.getObject(label)) as? T
    }

    inline operator fun <reified T> get(label: String): T? {
        return valueBy(label)
    }

    inline operator fun <reified T> get(idx: Int): T? {
        return valueAt(idx)
    }
}

private val jsonTypes: Set<String> = setOf("json", "JSON", "jsonb", "JSONB")


