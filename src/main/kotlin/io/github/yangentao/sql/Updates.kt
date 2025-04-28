package io.github.yangentao.sql

import java.sql.Connection

/**
 * Created by entaoyang@163.com on 2017/4/5.
 */

fun Connection.dropTable(tableName: String): Int {
    return this.update("DROP TABLE IF EXISTS ${tableName.escapeSQL}")
}

fun Connection.createTable(tableName: String, columns: List<String>, options: List<String> = emptyList()): Int {
    val sql = buildString {
        append("CREATE TABLE IF NOT EXISTS ${tableName.escapeSQL} (")
        append(columns.joinToString(", "))
        append(")")
        this += options.joinToString(", ")
    }
    return this.update(sql)
}

fun Connection.createIndex(tableName: String, columnName: String) {
    val idxName = "${tableName.unescapeSQL}_${columnName.unescapeSQL}_INDEX"
    exec("CREATE INDEX  $idxName ON ${tableName.escapeSQL}(${columnName.escapeSQL})")
}





