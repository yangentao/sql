@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.*
import kotlin.reflect.KClass
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

class DistinctExp : SQLExpress("DISTINCT") {
    fun ON(vararg columns: Any): DistinctExp {
        return ON_LIST(columns.toList())
    }

    fun ON_LIST(columns: List<Any>): DistinctExp {
        this.."ON"
        this.brace(columns)
        return this
    }
}

val DISTINCT: DistinctExp get() = DistinctExp()

fun DISTINCT_ON(vararg columns: Any): DistinctExp {
    return DISTINCT.ON_LIST(columns.toList())
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

infix fun String.AS(right: Any): SQLExpress {
    return SQLExpress(this).."AS"..right
}

infix fun PropSQL.AS(right: Any): SQLExpress {
    return SQLExpress(this).."AS"..right
}

infix fun BaseModelClass<*>.AS(right: Any): SQLExpress {
    return SQLExpress(this).."AS"..right
}

infix fun KClass<*>.AS(right: Any): SQLExpress {
    return SQLExpress(this).."AS"..right
}

infix fun <T : SQLExpress> T.AS(right: Any): T {
    return this.."AS"..right
}

fun <T : SQLExpress> T.AS(right: Any, columns: List<Any>): T {
    val e = this.."AS"..right
    if (columns.isNotEmpty()) e.brace(columns)
    return e
}

fun <T : SQLExpress> T.AS(block: () -> SQLExpress): T {
    return this.."AS ("..block()..")"
}



