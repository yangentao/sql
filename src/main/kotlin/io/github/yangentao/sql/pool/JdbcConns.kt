package io.github.yangentao.sql.pool

import org.sqlite.SQLiteDataSource
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

object DataSources {
    fun sqliteTemp(): SQLiteDataSource {
        return sqlite("")
    }

    fun sqliteMemory(): SQLiteDataSource {
        return sqlite(":memory:")
    }

    fun sqlite(file: String): SQLiteDataSource {
        Class.forName("org.sqlite.JDBC")
        val sqlite = SQLiteDataSource()
        sqlite.url = "jdbc:sqlite:$file"
        sqlite.setEncoding("UTF-8")
        return sqlite
    }
}

object JdbcConns {
    fun sqliteDataSource(file: String): SQLiteDataSource {
        Class.forName("org.sqlite.JDBC")
        val sqlite = SQLiteDataSource()
        sqlite.url = "jdbc:sqlite:$file"
        return sqlite
    }

    fun sqlite(file: String): Connection {
        Class.forName("org.sqlite.JDBC")
        return DriverManager.getConnection("jdbc:sqlite:$file")
    }

    fun postgreSQL(user: String, password: String, dbname: String, host: String, port: Int = 5432): Connection {
        Class.forName("org.postgresql.Driver")
        val p = Properties()
        p.putIfAbsent("user", user)
        p.putIfAbsent("password", password)
        return DriverManager.getConnection("jdbc:postgresql://${host}:$port/$dbname", p)
    }

    fun mySQL(user: String, password: String, dbname: String, host: String, port: Int = 3306): Connection {
        Class.forName("com.mysql.cj.jdbc.Driver")
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
