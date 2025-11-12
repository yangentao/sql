@file:Suppress("unused")

package io.github.yangentao.sql.pool

import java.sql.Connection
import java.sql.DriverManager
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

class DataSourceConnectionBuilder(val dataSource: DataSource) : ConnectionBuilder {
    override fun create(): Connection {
        return dataSource.connection
    }

    override fun destroy() {
    }
}

class PostgresConnectionBuilder(val user: String, val password: String, val dbname: String, val host: String, val port: Int = 5432) : ConnectionBuilder {
    init {
        Class.forName("org.postgresql.Driver")
    }

    override fun create(): Connection {
        val p = Properties()
        p.putIfAbsent("user", user)
        p.putIfAbsent("password", password)
        return DriverManager.getConnection("jdbc:postgresql://${host}:$port/$dbname", p)
    }
}

class MysqlConnectionBuilder(val user: String, val password: String, val dbname: String, val host: String, val port: Int = 3306) : ConnectionBuilder {
    init {
        Class.forName("com.mysql.cj.jdbc.Driver")
    }

    override fun create(): Connection {
        val p = Properties()
        p.putIfAbsent("user", user)
        p.putIfAbsent("password", password)
        p.putIfAbsent("useSSL", "true")
        p.putIfAbsent("useUnicode", "true")
        p.putIfAbsent("characterEncoding", "UTF-8")
        p.putIfAbsent("serverTimezone", "Hongkong")
        p.putIfAbsent("sessionVariables", "sql_mode=ANSI_QUOTES")
        return DriverManager.getConnection("jdbc:mysql://$host:$port/$dbname", p)
    }

}

class SqliteConnectionBuilder(val file: String) : ConnectionBuilder {
    init {
        Class.forName("org.sqlite.JDBC")
    }

    override fun create(): Connection {
        return DriverManager.getConnection("jdbc:sqlite:$file")
    }

    companion object {
        const val TEMP = ""
        const val MEMORY = ":memory:"

        fun memory(): SqliteConnectionBuilder {
            return SqliteConnectionBuilder(MEMORY)
        }

        fun temp(): SqliteConnectionBuilder {
            return SqliteConnectionBuilder(TEMP)
        }
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

    fun pushSource(ds: DataSource) {
        push(DataSourceConnectionBuilder(ds))
    }

    fun pushSource(name: String, ds: DataSource) {
        push(name, DataSourceConnectionBuilder(ds))
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