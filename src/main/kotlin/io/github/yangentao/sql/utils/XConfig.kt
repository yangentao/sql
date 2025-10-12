package io.github.yangentao.sql.utils

import io.github.yangentao.anno.Length
import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.clause.EQ
import io.github.yangentao.sql.filter

@Suppress("unused")
class XConfig : TableModel() {
    @ModelField(primaryKey = true, defaultValue = "")
    var group: String by model

    @ModelField(primaryKey = true)
    var name: String by model

    @ModelField
    @Length(max = 2048)
    var valueText: String? by model

    @ModelField
    var valueLong: Long? by model

    @ModelField
    var valueDouble: Double? by model

    companion object : TableModelClass<XConfig>() {
        const val DEFAULT_GROUP = ""

        fun group(g: String = DEFAULT_GROUP): XGroupConfig {
            return XGroupConfig(g)
        }

        fun getString(name: String): String? {
            return XConfig.filter(XConfig::group EQ DEFAULT_GROUP, XConfig::name EQ name).select(XConfig::valueText).oneValue()
        }

        fun getLong(name: String): Long? {
            return XConfig.filter(XConfig::group EQ DEFAULT_GROUP, XConfig::name EQ name).select(XConfig::valueLong).oneValue()
        }

        fun getInt(name: String): Int? {
            return getLong(name)?.toInt()
        }

        fun getDouble(name: String): Double? {
            return XConfig.filter(XConfig::group EQ DEFAULT_GROUP, XConfig::name EQ name).select(XConfig::valueDouble).oneValue()
        }

        fun put(name: String, value: String) {
            XConfig.upsert(XConfig::group to DEFAULT_GROUP, XConfig::name to name, XConfig::valueText to value)
        }

        fun put(name: String, value: Int) {
            put(name, value.toLong())
        }

        fun put(name: String, value: Long) {
            XConfig.upsert(XConfig::group to DEFAULT_GROUP, XConfig::name to name, XConfig::valueLong to value)
        }

        fun put(name: String, value: Double) {
            XConfig.upsert(XConfig::group to DEFAULT_GROUP, XConfig::name to name, XConfig::valueDouble to value)
        }

    }
}

@JvmInline
@Suppress("unused")
value class XGroupConfig(val group: String) {

    fun list(): List<XConfig> {
        return XConfig.filter(XConfig::group EQ group).list()
    }

    fun getString(name: String): String? {
        return XConfig.filter(XConfig::group EQ group, XConfig::name EQ name).select(XConfig::valueText).oneValue()
    }

    fun getLong(name: String): Long? {
        return XConfig.filter(XConfig::group EQ group, XConfig::name EQ name).select(XConfig::valueLong).oneValue()
    }

    fun getInt(name: String): Int? {
        return getLong(name)?.toInt()
    }

    fun getDouble(name: String): Double? {
        return XConfig.filter(XConfig::group EQ group, XConfig::name EQ name).select(XConfig::valueDouble).oneValue()
    }

    fun put(name: String, value: String) {
        XConfig.upsert(XConfig::group to group, XConfig::name to name, XConfig::valueText to value)
    }

    fun put(name: String, value: Int) {
        put(name, value.toLong())
    }

    fun put(name: String, value: Long) {
        XConfig.upsert(XConfig::group to group, XConfig::name to name, XConfig::valueLong to value)
    }

    fun put(name: String, value: Double) {
        XConfig.upsert(XConfig::group to group, XConfig::name to name, XConfig::valueDouble to value)
    }
}