@file:Suppress("unused")

package io.github.yangentao.sql

import io.github.yangentao.anno.ModelTable
import io.github.yangentao.anno.ModelView
import io.github.yangentao.anno.Name
import io.github.yangentao.types.or
import io.github.yangentao.types.ownerClass
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation

/**
 * Created by entaoyang@163.com on 2017/6/10.
 */

interface WithConnection {
    val connection: Connection
}

private const val MysqlKeywors =
    "ACCESSIBLE,ADD,ANALYZE,ASC,BEFORE,CASCADE,CHANGE,CONTINUE,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DESC,DISTINCTROW,DIV,DUAL,ELSEIF,EMPTY,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FIRST_VALUE,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERATED,GROUPS,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INT1,INT2,INT3,INT4,INT8,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,ITERATE,JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LEAD,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,NO_WRITE_TO_BINLOG,NTH_VALUE,NTILE,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OUTFILE,PURGE,READ,READ_WRITE,REGEXP,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SEPARATOR,SHOW,SIGNAL,SPATIAL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,UNDO,UNLOCK,UNSIGNED,USAGE,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,VIRTUAL,WHILE,WRITE,XOR,YEAR_MONTH,ZEROFILL"

private const val PostKeywords =
    "ALL,ANALYSE,ANALYZE,AND,ANY,ARRAY,AS,ASC,ASYMMETRIC,AUTHORIZATION,BINARY,BOTH,CASE,CAST,CHAR,CHARACTER,CHARACTERISTICS,CHECK,COLLATE,COLLATION,COLUMN,CONCURRENTLY,CREATE,CROSS,CURRENT_CATALOG,CURRENT_DATE,CURRENT_ROLE,CURRENT_SCHEMA,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,DEFAULT,DEFERRABLE,DESC,DISTINCT,DO,ELSE,END,EXCEPT,EXISTS,EXTRACT,FALSE,FETCH,FILTER,FOR,FOREIGN,FREEZE,FROM,GRANT,GROUP,HAVING,HOUR,ILIKE,IN,INITIALLY,INNER,INTERSECT,INTO,IS,ISNULL,JOIN,LATERAL,LEADING,LEFT,LIKE,LIMIT,LOCALTIME,LOCALTIMESTAMP,MINUTE,NATURAL,NOT,NOTNULL,NULL,NULLIF,NUMERIC,OFFSET,ON,ONLY,OR,ORDER,OUTER,OVER,OVERLAPS,PLACING,PRIMARY,REAL,REFERENCES,RETURNING,RIGHT,ROW,SELECT,SESSION_USER,SETOF,SIMILAR,SOME,SYMMETRIC,TABLE,TABLESAMPLE,THEN,TO,TRAILING,TRUE,UNION,USER,USING,VARIADIC,VERBOSE,WHEN,WHERE,WINDOW,WITH,WITHIN,WITHOUT,YEAR"

private val mysqlKeySet: Set<String> = MysqlKeywors.lowercase().split(',').map { it.trim() }.toSet()
private val postKeySet: Set<String> = PostKeywords.lowercase().split(',').map { it.trim() }.toSet()

private val sqlKeywordSet: Set<String> = mysqlKeySet + postKeySet

val KClass<*>.modelVersion: Int
    get() {
        return this.findAnnotation<ModelTable>()?.version ?: this.findAnnotation<ModelView>()?.version ?: 0
    }

val KClass<*>.nameSQL: String
    get() {
        return this.findAnnotation<Name>()?.value or this.simpleName!!.lowercase()
    }
val KProperty<*>.fieldSQL: String
    get() {
        return this.findAnnotation<Name>()?.value or this.name.lowercase()
    }
val KProperty<*>.fullNmeSQL: String
    get() {
        return "${this.ownerClass!!.nameSQL}.${this.fieldSQL}"
    }

val String.escapeSQL: String
    get() {
        if (this.isEmpty()) return this
        if (this.first() == '"' && this.last() == '"') return this
        if ('.' !in this) {
            if (this in sqlKeywordSet) {
                return "\"$this\""
            }
            return this
        }
        return this.split('.').joinToString(".") { it.escapeSQL }
    }
val String.unescapeSQL: String
    get() {
        if (this.isEmpty()) return this
        if (this.first() != '"' || this.last() != '"') return this
        if ('.' !in this) {
            return this.trim('"')
        }
        return this.split('.').joinToString(".") { it.unescapeSQL }
    }
