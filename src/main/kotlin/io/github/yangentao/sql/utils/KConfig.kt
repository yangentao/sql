package io.github.yangentao.sql.utils

import io.github.yangentao.anno.Length
import io.github.yangentao.anno.ModelField
import io.github.yangentao.anno.userName
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.clause.EQ
import io.github.yangentao.sql.filter
import kotlin.reflect.KProperty

class KConfig : TableModel() {

    @ModelField(primaryKey = true)
    @Length(max = 512)
    var name: String by model

    @ModelField
    @Length(max = 2048)
    var valueText: String? by model

    @ModelField
    var valueLong: Long? by model

    @ModelField
    var valueDouble: Double? by model

    companion object : TableModelClass<KConfig>() {
        operator fun <V> setValue(thisRef: Any?, property: KProperty<*>, value: V?) {
            val k = property.userName
            if (value == null) {
                remove(k)
            } else {
                when (value) {
                    is String -> put(k, value)
                    is Int -> put(k, value)
                    is Long -> put(k, value)
                    is Double -> put(k, value)
                    else -> put(k, value.toString())
                }
            }
        }

        inline operator fun <reified V> getValue(thisRef: Any?, property: KProperty<*>): V? {
            val k = property.userName
            return when (V::class) {
                String::class -> getString(k) as V?
                Int::class -> getInt(k) as V?
                Long::class -> getLong(k) as V?
                Double::class -> getDouble(k) as V?
                else -> error("ConfigKV.getValue() Not support type: ${V::class}")
            }
        }

        fun remove(key: String) {
            delete(KConfig::name EQ key)
        }

        fun listAll(): List<KConfig> {
            return list()
        }

        fun getString(key: String): String? {
            return filter(KConfig::name EQ key).select(KConfig::valueText).oneValue()
        }

        fun getInt(key: String): Int? {
            return filter(KConfig::name EQ key).select(KConfig::valueLong).oneValue()
        }

        fun getLong(key: String): Long? {
            return filter(KConfig::name EQ key).select(KConfig::valueLong).oneValue()
        }

        fun getDouble(key: String): Double? {
            return filter(KConfig::name EQ key).select(KConfig::valueDouble).oneValue()
        }

        fun put(key: String, value: String?) {
            upsert(KConfig::name to key, KConfig::valueText to value)
        }

        fun put(key: String, value: Int?) {
            upsert(KConfig::name to key, KConfig::valueLong to value)
        }

        fun put(key: String, value: Long?) {
            upsert(KConfig::name to key, KConfig::valueLong to value)
        }

        fun put(key: String, value: Double?) {
            upsert(KConfig::name to key, KConfig::valueDouble to value)
        }
    }
}
