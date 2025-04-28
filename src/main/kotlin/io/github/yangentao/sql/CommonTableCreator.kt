package io.github.yangentao.sql


import io.github.yangentao.anno.Comment
import io.github.yangentao.anno.Label
import io.github.yangentao.anno.Length
import io.github.yangentao.kson.KsonArray
import io.github.yangentao.kson.KsonObject
import io.github.yangentao.reflect.quotedSingle
import java.sql.Connection
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation

fun Connection.tableCreator(cls: KClass<*>): CommonTableCreator {
    if (isSQLite) return SQLiteTableCreator(this, cls)
    if (isMySQL) return MySQLTableCreator(this, cls)
    if (isPostgres) return PostgrsTableCreator(this, cls)
    return CommonTableCreator(this, cls)
}

class ColumnInfo(val prop: KProperty<*>, val fieldAnno: ModelField) {
    val fieldClass: KClass<*> = prop.returnType.classifier as KClass<*>
    val fieldName: String = prop.fieldSQL

    val primaryKey: Boolean = fieldAnno.primaryKey
    val index: Boolean = fieldAnno.index || prop.hasAnnotation<ForeignKey>()
    val unique: Boolean = fieldAnno.unique
    val uniqueName: String = fieldAnno.uniqueName
    val autoInc: Boolean = fieldAnno.autoInc > 0
    val autoIncBase: Int = fieldAnno.autoInc
    val notNull: Boolean = fieldAnno.notNull || !prop.returnType.isMarkedNullable
    val defaultValue: String? = if (fieldAnno.defaultValue.trim().isEmpty()) null else fieldAnno.defaultValue.trim()
    val commment: String? = prop.findAnnotation<Comment>()?.value ?: prop.findAnnotation<Label>()?.value
}

open class CommonTableCreator(val connection: Connection, val cls: KClass<*>) {
    val tableName: String = cls.nameSQL
    val columns: List<ColumnInfo> by lazy { cls.propertiesModel.map { ColumnInfo(it.property, it.annotation) } }
    val primaryKeyCount: Int by lazy { columns.count { it.primaryKey } }
    val tableComment: String? = cls.findAnnotation<Comment>()?.value ?: cls.findAnnotation<Label>()?.value

    fun createTable() {
        connection.exec(defineTable())
        createExtras()
        val idxList = columns.filter { it.index }
        for (idx in idxList) {
            connection.createIndex(tableName, idx.fieldName)
        }
    }

    //其他, 索引等
    open fun createExtras() {
        val c = columns.firstOrNull { it.autoInc && it.autoIncBase > 1 }
        if (c != null) {
            createAutoIncrementBase(c)
        }
    }

    //自增, 开始值
    open fun createAutoIncrementBase(col: ColumnInfo) {

    }

    open fun defineTable(): String {
        return buildString {
            val cs = columns.map { defineColumn(it) } + defineExtraColumns()
            append("CREATE TABLE IF NOT EXISTS ${tableName.escapeSQL} (")
            append(cs.joinToString(", "))
            append(")")
            this.append(defineTableOptions().joinToString(", "))
        }
    }

    //定义class中为体现的, 用逗号区分的, 比如 PRIMARY KEY(id, ver) 或 UNIQUE (firstname,name)
    open fun defineExtraColumns(): List<String> {
        val ls = ArrayList<String>()
        if (primaryKeyCount > 1) {
            val pks = columns.filter { it.primaryKey }.joinToString(", ") { it.prop.fieldSQL.escapeSQL }
            ls += "PRIMARY KEY($pks)"
        }

        val uniq2 = columns.filter { it.uniqueName.isNotEmpty() }.groupBy { it.uniqueName }
        for ((_, uls) in uniq2) {
            val s = uls.joinToString(",") { it.fieldName.escapeSQL }
            ls += "UNIQUE ($s) "
        }
        return ls
    }

    //直接跟随在 create table() 后的
    open fun defineTableOptions(): List<String> {
        return emptyList()
    }

    //id INTEGER NOT NULL AUTOINCREMENT
    open fun defineColumn(col: ColumnInfo): String {
        val ls = ArrayList<String>()
        ls += col.prop.fieldSQL.escapeSQL
        ls += typeName(col)
        if (col.notNull) {
            ls += "NOT NULL"
        }
        if (col.unique && col.uniqueName.isEmpty()) {
            ls += "UNIQUE"
        }
        if (col.primaryKey && primaryKeyCount == 1) {
            ls += "PRIMARY KEY"
        }
        if (col.autoInc) {
            ls += autoIncrementKey()
        }
        if (col.defaultValue != null) {
            ls += "DEFAULT ${col.defaultValue}"
        }
        afterDefineColumn(col)?.also { ls += it }
        return ls.filter { it.isNotEmpty() }.joinToString(" ")
    }

    open fun afterDefineColumn(col: ColumnInfo): String? {
        return null
    }

    open fun autoIncrementKey(): String {
        return "AUTO_INCREMENT"
    }

    open fun longTextKey(): String {
        return "TEXT"
    }

    open fun typeName(col: ColumnInfo): String {
        val decimal = col.prop.findAnnotation<Decimal>()
        when (col.fieldClass) {
            Boolean::class -> return "BOOLEAN"
            Int::class, Short::class, Byte::class -> return "INTEGER"
            Long::class -> return "BIGINT"
            Float::class -> return if (decimal != null) "DECIMAL(${decimal.precision},${decimal.scale})" else "REAL"
            Double::class -> return if (decimal != null) "DECIMAL(${decimal.precision},${decimal.scale})" else "DOUBLE PRECISION"

            Time::class, LocalTime::class -> return "TIME"
            Date::class, java.util.Date::class, LocalDate::class -> return "DATE"
            Timestamp::class, LocalDateTime::class -> return "TIMESTAMP"

            KsonObject::class, KsonArray::class -> return "TEXT"

            String::class -> {
                val lenAnno = col.prop.findAnnotation<Length>() ?: return "VARCHAR(255)"
                if (lenAnno.fixed > 0) {
                    return "CHAR(${lenAnno.fixed})"
                }
                if (lenAnno.max > 0) {
                    return if (lenAnno.max < 65535) {
                        "VARCHAR(${lenAnno.max})"
                    } else {
                        longTextKey()
                    }
                }
                return "VARCHAR(255)"
            }

            ByteArray::class -> {
                val lenAnno = col.prop.findAnnotation<Length>() ?: return "BLOB"
                return if (lenAnno.max < 65535) "BLOB" else "LONGBLOB"
            }

            BooleanArray::class, ShortArray::class, IntArray::class, LongArray::class, FloatArray::class, DoubleArray::class -> return "TEXT"
            Array<Boolean>::class, Array<Short>::class, Array<Int>::class, Array<Long>::class, Array<Float>::class, Array<Double>::class, Array<String>::class -> return "TEXT"
            else -> {
                error("Type NOT Support: ${col.fieldName}, ${col.fieldClass}")
            }
        }
    }

}

//AUTO_INCREMENT
class MySQLTableCreator(connection: Connection, cls: KClass<*>) : CommonTableCreator(connection, cls) {
    override fun createAutoIncrementBase(col: ColumnInfo) {
        connection.exec("ALTER TABLE ${tableName.escapeSQL} AUTO_INCREMENT = ${col.autoIncBase}")
    }

    override fun afterDefineColumn(col: ColumnInfo): String? {
        val comment = col.commment
        if (comment != null && comment.isNotEmpty()) return " COMMENT " + comment.quotedSingle
        return null
    }

    override fun longTextKey(): String {
        return "LONGTEXT"
    }

    override fun defineTableOptions(): List<String> {
        val ls = ArrayList<String>()
        val commentTable = tableComment
        if (commentTable != null) {
            ls += "COMMENT=${commentTable.quotedSingle}"
        }
        return ls
    }

    override fun typeName(col: ColumnInfo): String {
        when (col.fieldClass) {
            KsonObject::class, KsonArray::class -> return "JSON"
        }
        return super.typeName(col)
    }
}

class PostgrsTableCreator(connection: Connection, cls: KClass<*>) : CommonTableCreator(connection, cls) {

    override fun createExtras() {
        if (tableComment != null) {
            connection.exec("COMMENT ON TABLE ${tableName.escapeSQL} IS ${tableComment.quotedSingle}")
        }
        for (col in columns) {
            if (col.commment == null) continue
            connection.exec("COMMENT ON COLUMN ${tableName.escapeSQL}.${col.fieldName} IS ${col.commment.quotedSingle}")
        }
    }

    override fun createAutoIncrementBase(col: ColumnInfo) {
        connection.exec("ALTER SEQUENCE ${tableName + "_" + col.fieldName.escapeSQL + "_seq"} RESTART WITH ${col.autoIncBase} INCREMENT BY 1")
    }

    override fun autoIncrementKey(): String {
        return ""
    }

    override fun typeName(col: ColumnInfo): String {
        val fixLen: Long = col.prop.findAnnotation<Length>()?.fixed ?: 0L
        val arrayDef: String = if (fixLen > 0) "[$fixLen]" else "[]"

        when (col.fieldClass) {
            Int::class, Short::class, Byte::class -> if (col.autoInc) {
                return "SERIAL"
            }

            Long::class -> if (col.autoInc) {
                return "BIGSERIAL"
            }

            KsonObject::class, KsonArray::class -> return "JSON"
            ByteArray::class -> return "BYTEA"

            BooleanArray::class -> return "BOOLEAN$arrayDef"
            Array<Boolean>::class -> return "BOOLEAN$arrayDef"
            ShortArray::class -> return "INTEGER$arrayDef"
            Array<Short>::class -> return "INTEGER$arrayDef"
            IntArray::class -> return "INTEGER$arrayDef"
            Array<Int>::class -> return "INTEGER$arrayDef"
            LongArray::class -> return "BIGINT$arrayDef"
            Array<Long>::class -> return "BIGINT$arrayDef"
            FloatArray::class -> return "REAL$arrayDef"
            Array<Float>::class -> return "REAL$arrayDef"
            DoubleArray::class -> return "DOUBLE$arrayDef"
            Array<Double>::class -> return "DOUBLE$arrayDef"
            Array<String>::class -> return "TEXT$arrayDef"
        }
        return super.typeName(col)
    }
}

//自增
//自增问题,  https://boldena.com/article/59061
//https://sqlite.org/datatype3.html
class SQLiteTableCreator(connection: Connection, cls: KClass<*>) : CommonTableCreator(connection, cls) {
    override fun createAutoIncrementBase(col: ColumnInfo) {
        connection.sqliteChangeSequence(tableName, col.autoIncBase)
    }

    override fun autoIncrementKey(): String {
        return "AUTOINCREMENT"
    }

    override fun typeName(col: ColumnInfo): String {
        when (col.fieldClass) {
            Long::class, Int::class, Short::class, Byte::class -> return "INTEGER"
            ByteArray::class -> return "BLOB"
            KsonObject::class, KsonArray::class -> return "TEXT"
        }
        return super.typeName(col)
    }

}

fun Connection.sqliteChangeSequence(tableName: String, newSeq: Int) {
    val tab = tableName.escapeSQL.quotedSingle
    val rs = this.query("SELECT name, seq FROM sqlite_sequence WHERE name = $tab")
    if (rs.next()) {
        this.exec("UPDATE sqlite_sequence SET seq = $newSeq WHERE name = $tab")
    } else {
        this.insert("INSERT INTO sqlite_sequence(name, seq) VALUES( $tab, $newSeq)", emptyList())
    }
    rs.close()
}