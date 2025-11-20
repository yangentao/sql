@file:Suppress("unused")

package io.github.yangentao.sql.pool

import java.sql.Connection
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class ConnectionName(val value: String)

val KClass<*>.connectionName: String? get() = this.findAnnotation<ConnectionName>()?.value
val KClass<*>.namedConnection: Connection get() = HarePool.pick(this.connectionName)

interface ConnectionBuilder {
    fun create(): Connection
    fun destroy() {}
}

class DataSourceBuilder(val dataSource: DataSource) : ConnectionBuilder {
    override fun create(): Connection {
        return dataSource.connection
    }

    override fun destroy() {
    }
}

const val NO_NAME_CONNECTION = "no_name"

interface NamedConnections {
    fun push(name: String, builder: ConnectionBuilder)

    fun pickOne(name: String): Connection

    fun pick(name: String?): Connection {
        if (name == null || name.isEmpty()) {
            return pickOne(NO_NAME_CONNECTION)
        }
        return pickOne(name)
    }

    fun push(builder: ConnectionBuilder) {
        push(NO_NAME_CONNECTION, builder)
    }

    fun pick(): Connection {
        return pick(NO_NAME_CONNECTION)
    }

    fun pushSource(source: DataSource) {
        push(DataSourceBuilder(source))
    }

    fun pushSource(name: String, source: DataSource) {
        push(name, DataSourceBuilder(source))
    }

    fun destroy() {}
}

object HarePool : NamedConnections {
    private var hp: NamedConnections = EntaoPool

    fun noPool() {
        hp = DelegatePool()
    }

    override fun push(name: String, builder: ConnectionBuilder) {
        hp.push(name, builder)
    }

    override fun pickOne(name: String): Connection {
        return hp.pickOne(name)
    }

    override fun destroy() {
        hp.destroy()
    }

    fun pushSqliteMemory() {
        pushSource(LiteSources.sqliteMemory())
    }
}

class DelegatePool : NamedConnections {
    private val dataSourceMap: HashMap<String, ConnectionBuilder> = HashMap()
    override fun push(name: String, builder: ConnectionBuilder) {
        dataSourceMap[name] = builder
    }

    override fun pickOne(name: String): Connection {
        return dataSourceMap[name]!!.create()
    }

    override fun destroy() {
        val ls = dataSourceMap.values.toList()
        for (b in ls) {
            b.destroy()
        }
        dataSourceMap.clear()
    }

}