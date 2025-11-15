@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.*
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

typealias PropSQL = KProperty<*>

internal val newExp: SQLExpress get() = SQLExpress(null)

internal val Any.asValue: SQLExpress
    get() {
        return when (this) {
            is String -> newExp.."?" withArg this
            is Date -> newExp.."?" withArg this
            is PropSQL -> SQLExpress(this.fullNmeSQL)
            is SQLExpress -> this
            else -> newExp..this
        }
    }
val Any.asExpress: SQLExpress
    get() {
        return when (this) {
            is String -> newExp..this.escapeSQL
            is SQLExpress -> this
            else -> newExp..this
        }
    }
internal val Any.asColumn: String
    get() {
        return when (this) {
            is String -> this.escapeSQL
            is PropSQL -> this.fieldSQL.escapeSQL
            else -> error("Column name only support String or Property")
        }
    }

val <T : BaseModel> BaseModelClass<T>.ALL get() = this.tableClass.nameSQL.escapeSQL + ".*"

open class SQLExpress(sqlClause: String? = null, args: ArgList = emptyList()) {
    val buffer: StringBuilder = StringBuilder(128)
    val arguments: ArrayList<Any?> = ArrayList()

    init {
        if (sqlClause != null) buffer.append(sqlClause.trim())
        if (args.isNotEmpty()) arguments.addAll(args)
    }

    val isEmpty: Boolean get() = sql.isEmpty()
    val isNotEmpty: Boolean get() = sql.isNotEmpty()
    val sql: String get() = buffer.toString().trim()

    fun addAll(exps: List<SQLExpress>, sep: String, transform: ((SQLExpress) -> String)? = null) {
        buffer.append(' ')
        buffer.append(exps.filter { it.isNotEmpty }.joinToString(sep, transform = transform ?: { it.sql }))
        for (e in exps) {
            arguments.addAll(e.arguments)
        }
    }

    fun append(express: Any, sep: String = " "): SQLExpress {
        buffer.append(sep)
        when (express) {
            is String -> buffer.append(express.trim())
            is SQLExpress -> {
                buffer.append(express.sql)
                arguments.addAll(express.arguments)
            }

            is Number -> buffer.append(express.toString())
            is PropSQL -> buffer.append(express.fullNmeSQL.escapeSQL)
            is KClass<*> -> buffer.append(express.nameSQL.escapeSQL)
            is BaseModelClass<*> -> buffer.append(express.tableClass.nameSQL.escapeSQL)
            is List<*> -> this.addList(express.filterNotNull())
            is Set<*> -> this.addList(express.filterNotNull())
            is Array<*> -> this.addList(express.filterNotNull())
            else -> error("not support value: $express,  SQLExpress.add(express)")
        }
        return this
    }

    override fun toString(): String {
        return sql
    }

    fun dump() {
        sqlLog.d("SQL: $this")
        if (arguments.isNotEmpty()) {
            sqlLog.d("Arguments: " + arguments.joinToString(", "))
        }
    }
}

val <T : SQLExpress> T.braced: T
    get() {
        this.buffer.insert(0, '(')
        this.buffer.append(')')
        return this
    }

infix fun <T : SQLExpress> T.withArg(arg: Any): T {
    arguments.add(arg)
    return this
}

@Deprecated("user withArg instead")
infix fun <T : SQLExpress> T.addArg(arg: Any): T {
    arguments.add(arg)
    return this
}

fun <T : SQLExpress> T.withArgs(args: ArgList?): T {
    if (args != null && args.isNotEmpty()) arguments.addAll(args)
    return this
}

@Deprecated("user withArgs instead")
fun <T : SQLExpress> T.addArgs2(args: ArgList?): T {
    if (args != null && args.isNotEmpty()) arguments.addAll(args)
    return this
}

fun <T : SQLExpress> T.addList(exps: Collection<Any>, joinString: String = ","): T {
    if (exps.isEmpty()) return this
    for ((n, item) in exps.withIndex()) {
        if (n != 0) this..joinString
        this..item
    }
    return this
}

fun <T : SQLExpress, V> T.addList(ls: Collection<V>, joinString: String = ",", block: (T, V) -> Unit): T {
    if (ls.isEmpty()) return this
    for ((n, item) in ls.withIndex()) {
        if (n != 0) this..joinString
        block(this, item)
    }
    return this
}

infix operator fun <T : SQLExpress> T.rangeTo(express: Any): T {
    this.append(express)
    return this
}

@Deprecated("use rangeTo instead.  a..b ")
infix fun <T : SQLExpress> T.add(express: Any): T {
    this.append(express)
    return this
}

class DistinctExp : SQLExpress("DISTINCT")
class DistinctOnExp(columns: List<Any>) : SQLExpress() {
    init {
        this.."DISTINCT ON("
        this.addList(columns) { e, item ->
            e..item.asExpress
        }
        this..")"
    }
}

val DISTINCT: DistinctExp = DistinctExp()

fun DISTINCT_ON(vararg columns: Any): DistinctOnExp {
    return DistinctOnExp(columns.toList())
}

infix fun SQLNode.UNION(right: SQLExpress): SQLNode {
    return this.."UNION"..right
}

infix fun SQLNode.UNION_ALL(right: SQLExpress): SQLNode {
    return this.."UNION_ALL"..right
}

infix fun SQLNode.INTERSECT(right: SQLExpress): SQLNode {
    return this.."INTERSECT"..right
}

val String.ASC: String get() = "${this.escapeSQL} ASC"
val String.DESC: String get() = "${this.escapeSQL} DESC"

val PropSQL.ASC: String get() = "${this.fullNmeSQL} ASC"
val PropSQL.DESC: String get() = "${this.fullNmeSQL} DESC"

fun CASE_COLUMN(column: Any, valueResults: List<Pair<Any, Any>>, elseValue: Any? = null): SQLExpress {
    val exp = newExp
    exp.."CASE"..column
    for (p in valueResults) {
        exp.."WHEN"..p.first.."THEN"
        if (p.second is String) {
            exp.."?"
            exp.arguments.add(p.second)
        } else {
            exp..p.second
        }
    }
    if (elseValue != null) exp.."ELSE"..elseValue
    exp.."END"
    return exp
}

fun CASE_CONDITION(conditionResults: List<Pair<Where, Any>>, elseValue: Any? = null): SQLExpress {
    val exp = newExp
    exp.."CASE"
    for (p in conditionResults) {
        exp.."WHEN"..p.first.."THEN"
        if (p.second is String) {
            exp.."?"
            exp.arguments.add(p.second)
        } else {
            exp..p.second
        }
    }
    if (elseValue != null) {
        exp.."ELSE"
        if (elseValue is String) {
            exp.."?"
            exp.arguments.add(elseValue)
        } else {
            exp..elseValue
        }
    }
    return exp
}

fun COALESCE(exp: Any, defaultValue: Any): SQLExpress {
    return newExp.."COALESCE("..exp..","..defaultValue..")"
}

fun LEAST(vararg exps: Any): SQLExpress {
    return newExp.."LEAST("..exps..")"
}

fun GREATEST(vararg exps: Any): SQLExpress {
    return newExp.."GREATEST("..exps..")"
}





