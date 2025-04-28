package io.github.yangentao.sql


import io.github.yangentao.anno.AutoCreateTable
import io.github.yangentao.sql.pool.namedConnection
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf

class TableMigrater(private val conn: Connection, private val cls: KClass<*>) {
    private val tableName: String = cls.nameSQL
    val tableCreator: CommonTableCreator = conn.tableCreator(cls)

    init {
        if (cls.isSubclassOf(TableModel::class)) {
            if (cls.findAnnotation<AutoCreateTable>()?.value != false) {
                if (!conn.tableExists(tableName)) {
                    createTable(conn)
                } else {
                    mergeTable()
                    mergeIndex()
                }
            }
        } else {
            error("Migrate type error, not table model")
        }
    }

    private fun mergeIndex() {
        val oldIdxs = conn.tableIndexList(tableName).map { it.COLUMN_NAME.unescapeSQL }.toSet()
        val newIdxs = tableCreator.columns.filter { it.index }
        for (p in newIdxs) {
            if (p.fieldName !in oldIdxs) {
                conn.createIndex(tableName, p.fieldName)
            }
        }
    }

    private fun mergeTable() {
        val colList = conn.tableDesc(tableName)
        val cols: Set<String> = colList.map { it.columnName.unescapeSQL }.toSet()
        for (p in tableCreator.columns) {
            if (p.fieldName !in cols) {
                val s = tableCreator.defineColumn(p)
                conn.exec("ALTER TABLE ${tableName.escapeSQL} ADD COLUMN $s")
            }
        }
    }

    private fun createTable(conn: Connection) {
        tableCreator.createTable()
    }

    companion object {
        private val tableCreateSet = HashSet<KClass<*>>()

        fun dataSourceChanged() {
            tableCreateSet.clear()
        }

        @Synchronized
        fun migrate(vararg tables: KClass<out TableModel>) {
            for (table in tables) {
                if (table !in tableCreateSet) {
                    tableCreateSet.add(table)
                    TableMigrater(table.namedConnection, table)
                }
            }
        }
    }
}
