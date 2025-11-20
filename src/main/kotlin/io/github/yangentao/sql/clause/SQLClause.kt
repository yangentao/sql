@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.*
import kotlin.reflect.KProperty

typealias PropSQL = KProperty<*>

internal val Any.asColumn: String
    get() {
        return when (this) {
            is String -> this.escapeSQL
            is PropSQL -> this.fieldSQL.escapeSQL
            else -> error("Column name only support String or Property")
        }
    }

val <T : BaseModel> BaseModelClass<T>.ALL get() = this.tableClass.nameSQL.escapeSQL + ".*"

class DistinctExp : SQLExpress("DISTINCT")
class DistinctOnExp(columns: List<Any>) : SQLExpress() {
    init {
        this.."DISTINCT ON"
        this.parenthesed(columns)
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

val String.ASC: SQLExpress get() = SQLExpress(this).."ASC"
val String.DESC: SQLExpress get() = SQLExpress(this).."DESC"

val PropSQL.ASC: SQLExpress get() = SQLExpress(this).."ASC"
val PropSQL.DESC: SQLExpress get() = SQLExpress(this).."DESC"

fun CASE_COLUMN(column: Any, valueResults: List<Pair<Any, Any>>, elseValue: Any? = null): SQLExpress {
    val exp = SQLExpress("CASE")
    exp..column
    for (p in valueResults) {
        exp.."WHEN"..p.first.."THEN"..<p.second
    }
    if (elseValue != null) exp.."ELSE"..elseValue
    exp.."END"
    return exp
}

fun CASE_CONDITION(conditionResults: List<Pair<Where, Any>>, elseValue: Any? = null): SQLExpress {
    val exp = SQLExpress("CASE")
    for (p in conditionResults) {
        exp.."WHEN"..p.first.."THEN"..<p.second
    }
    if (elseValue != null) {
        exp.."ELSE"..<elseValue
    }
    return exp
}

fun COALESCE(exp: Any, defaultValue: Any): SQLExpress {
    return SQLExpress("COALESCE(")..exp..","..defaultValue..")"
}






