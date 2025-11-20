package io.github.yangentao.sql.pool

import java.sql.Connection
import java.sql.DriverManager
import java.util.*

class PostgresBuilder(val user: String, val password: String, val dbname: String, val host: String, val port: Int = 5432) : ConnectionBuilder {
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

class MysqlBuilder(val user: String, val password: String, val dbname: String, val host: String, val port: Int = 3306) : ConnectionBuilder {
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

class SqliteBuilder(val file: String) : ConnectionBuilder {
    init {
        Class.forName("org.sqlite.JDBC")
    }

    override fun create(): Connection {
        return DriverManager.getConnection("jdbc:sqlite:$file")
    }

    companion object {
        const val TEMP = ""
        const val MEMORY = ":memory:"

        fun memory(): SqliteBuilder {
            return SqliteBuilder(MEMORY)
        }

        fun temp(): SqliteBuilder {
            return SqliteBuilder(TEMP)
        }
    }
}
