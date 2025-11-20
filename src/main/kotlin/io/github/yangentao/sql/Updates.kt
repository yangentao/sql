package io.github.yangentao.sql

import io.github.yangentao.sql.clause.CREATE_INDEX
import io.github.yangentao.sql.clause.CREATE_TABLE
import io.github.yangentao.sql.clause.DROP_TABLE
import java.sql.Connection

/**
 * Created by entaoyang@163.com on 2017/4/5.
 */

fun Connection.dropTable(tableName: String): Int {
    return this.update(DROP_TABLE(tableName))
}

fun Connection.createTable(tableName: String, columns: List<String>, options: List<String> = emptyList()): Int {
    val sql = CREATE_TABLE(tableName, columns, options)
    return this.update(sql)
}

fun Connection.createIndex(tableName: String, columnName: String) {
    val sql = CREATE_INDEX(tableName, columnName)
    exec(sql)
}





