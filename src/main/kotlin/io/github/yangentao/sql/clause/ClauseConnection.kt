@file:Suppress("unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.*
import io.github.yangentao.sql.pool.namedConnection
import java.sql.Connection
import java.sql.ResultSet

fun SQLExpress.insert(connection: Connection): InsertResult {
    return connection.insert(sql, arguments)
}

fun SQLExpress.update(connection: Connection): Int {
    return connection.update(this.sql, this.arguments)
}

fun SQLExpress.query(connection: Connection): ResultSet {
    return connection.query(sql, arguments)
}

inline fun <reified T : BaseModel> SQLExpress.query(): ResultSet {
    return query(T::class.namedConnection)
}

inline fun <reified T : BaseModel> SQLExpress.update(): Int {
    return update(T::class.namedConnection)
}

inline fun <reified T : BaseModel> SQLExpress.insert(): InsertResult {
    return insert(T::class.namedConnection)
}

context(wc: WithConnection)
fun SQLExpress.query(): ResultSet {
    return query(wc.connection)
}

context(wc: WithConnection)
fun SQLExpress.insert(): InsertResult {
    return insert(wc.connection)
}

context(wc: WithConnection)
fun SQLExpress.update(): Int {
    return update(wc.connection)
}
