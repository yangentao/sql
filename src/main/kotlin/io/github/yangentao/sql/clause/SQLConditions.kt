@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.ArgList
import io.github.yangentao.sql.escapeSQL
import io.github.yangentao.sql.fieldSQL
import io.github.yangentao.sql.modelFieldSQL
import io.github.yangentao.types.quoted

//https://sqlite.org/optoverview.html#the_between_optimization
private val likeAllowChars: Set<Char> = setOf('%', '[', ']', '-', '_', '^')
private val likeDenyChars: Set<Char> = setOf(' ', '\"', '\'', ';', ',', '*', '$', '/', '?', ':', '<', '>', '#', '(', ')', '{', '}')

open class Where(condition: String? = null) : SQLExpress(condition)

class WhereAnd(ws: List<Where>) : Where() {
    init {
        mergeSQL(ws, " AND ") {
            if (it is WhereOr) it.braced.sql else it.sql
        }
        mergeArgs(ws)
    }
}

class WhereOr(ws: List<Where>) : Where() {
    init {
        mergeSQL(ws, " OR ")
        mergeArgs(ws)
    }
}

class WhereOper(left: String, op: String, right: String?, args: ArgList? = null) : Where() {
    init {
        addSQL("$left $op $right")
        addArgList(args)
    }
}

class WhereExp(exp: String, args: ArgList? = null) : Where() {
    init {
        append(exp)
        addArgList(args)
    }
}

private val newWhere: Where get() = Where()

fun AND_ALL(vararg conditons: Where?): Where {
    return WhereAnd(conditons.filterNotNull());
}

fun AND_ALL(conditons: List<Where?>): Where {
    return WhereAnd(conditons.filterNotNull());
}

fun OR_ALL(conditons: List<Where?>): Where {
    return WhereOr(conditons.filterNotNull())
}

infix fun Where?.AND(other: Where?): Where {
    return WhereAnd(listOfNotNull(this, other))
}

infix fun Where?.OR(other: Where?): Where {
    return WhereOr(listOfNotNull(this, other))
}

fun NOT(other: Where): Where {
    assert(other.isNotEmpty)
    return WhereExp("NOT(${other.sql})", other.arguments)
}

fun EQ_ALL_X(vararg cs: Pair<PropSQL, Any?>): Where {
    return EQ_ALL(cs.toList())
}

fun EQ_ALL(cs: List<Pair<PropSQL, Any?>>): Where {
    return AND_ALL(cs.map { it.first EQ it.second })
}

infix fun String.EQ(value: Any?): Where {
    if (value == null) return IS_NULL(this.asKey)
    return newWhere..this.asKey.."="..value.asValue
}

infix fun PropSQL.EQ(value: Any?): Where {
    if (value == null) return IS_NULL(this.asKey)
    return newWhere..this.asKey.."="..value.asValue
}

infix fun String.NE(value: Any?): Where {
    if (value == null) return IS_NOT_NULL(this)
    return newWhere..this.asKey.."<>"..value.asValue
}

infix fun PropSQL.NE(value: Any): Where {
    return newWhere..this.asKey.."<>"..value.asValue
}

infix fun String.LE(value: Any): Where {
    return newWhere..this.asKey.."<="..value.asValue
}

infix fun PropSQL.LE(value: Any): Where {
    return newWhere..this.asKey.."<="..value.asValue
}

infix fun String.GE(value: Any): Where {
    return newWhere..this.asKey..">="..value.asValue
}

infix fun PropSQL.GE(value: Any): Where {
    return newWhere..this.asKey..">="..value.asValue
}

infix fun String.LT(value: Any): Where {
    return newWhere..this.asKey.."<"..value.asValue
}

infix fun PropSQL.LT(value: Any): Where {
    return newWhere..this.asKey.."<"..value.asValue
}

infix fun String.GT(value: Any): Where {
    return newWhere..this.asKey..">"..value.asValue
}

infix fun PropSQL.GT(value: Any): Where {
    return newWhere..this.asKey..">"..value.asValue
}

infix fun String.IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NULL(this.asKey)
    if (values.size == 1) return this.EQ(values.first())
    val c = newWhere..this.asKey.."IN("
    c.addList(values) { e, v ->
        e..v.asValue
    }
    return c..")"
}

infix fun PropSQL.IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NULL(this.asKey)
    if (values.size == 1) return this.EQ(values.first())
    val c = newWhere..this.asKey.."IN("
    c.addList(values) { e, v ->
        e..v.asValue
    }
    return c..")"
}

infix fun String.NOT_IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NOT_NULL(this.asKey)
    if (values.size == 1) return this.NE(values.first())
    val c = newWhere..this.asKey.."NOT IN("
    c.addList(values) { e, v ->
        e..v.asValue
    }
    return c..")"
}

infix fun PropSQL.NOT_IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NOT_NULL(this.asKey)
    if (values.size == 1) return this.NE(values.first())
    val c = newWhere..this.asKey.."NOT IN("
    c.addList(values) { e, v ->
        e..v.asValue
    }
    return c..")"
}

fun String.BETWEEN(minValue: Any, maxValue: Any): Where {
    return newWhere..this.asKey.."BETWEEN"..minValue.asValue.."AND"..maxValue.asValue
}

fun PropSQL.BETWEEN(minValue: Any, maxValue: Any): Where {
    return newWhere..this.asKey.."BETWEEN"..minValue.asValue.."AND"..maxValue.asValue
}

infix fun String.LIKE(pattern: String): Where {
    val s = pattern.filter { it !in likeDenyChars }
    return newWhere..this.asKey.."LIKE".."'$s'"
}

infix fun PropSQL.LIKE(pattern: String): Where {
    val s = pattern.filter { it !in likeDenyChars }
    return newWhere..this.asKey.."LIKE".."'$s'"
}

infix fun String.CONTAINS(value: String): Where {
    val s = value.filter { it !in likeDenyChars + likeAllowChars }
    return newWhere..this.asKey.."LIKE".."'%$s%'"
}

infix fun PropSQL.CONTAINS(value: String): Where {
    val s = value.filter { it !in likeDenyChars + likeAllowChars }
    return newWhere..this.asKey.."LIKE".."'%$s%'"
}

fun IS_NULL(exp: String): Where {
    return WhereExp("$exp IS NULL")
}

fun IS_NULL(exp: PropSQL): Where {
    return WhereExp("${exp.fieldSQL.escapeSQL} IS NULL")
}

fun IS_NULL(exp: SQLExpress): Where {
    return WhereExp("${exp.sql} IS NULL")
}

fun IS_NOT_NULL(exp: String): Where {
    return WhereExp("$exp IS NOT NULL")
}

fun IS_NOT_NULL(exp: PropSQL): Where {
    return WhereExp("${exp.fieldSQL.escapeSQL} IS NOT NULL")
}

fun IS_NOT_NULL(exp: SQLExpress): Where {
    return WhereExp("${exp.sql} IS NOT NULL")
}

fun EXISTS(exp: String): Where {
    return WhereExp("EXISTS $exp")
}

fun EXISTS(exp: PropSQL): Where {
    return WhereExp("EXISTS ${exp.fieldSQL.escapeSQL}")
}

fun EXISTS(exp: SQLExpress): Where {
    return WhereExp("EXISTS ${exp.sql}")
}

fun NOT_EXISTS(exp: String): Where {
    return WhereExp("NOT EXISTS $exp")
}

fun NOT_EXISTS(exp: PropSQL): Where {
    return WhereExp("NOT EXISTS ${exp.fieldSQL.escapeSQL}")
}

fun NOT_EXISTS(exp: SQLExpress): Where {
    return WhereExp("NOT EXISTS ${exp.sql}")
}

fun ANY_OF(exp: Any): Where {
    return newWhere.."ANY("..exp..")"
}

fun ALL_OF(exp: Any): Where {
    return newWhere.."ALL("..exp..")"
}

infix fun String.HAS_ALL_BITS(value: Int): Where {
    return newWhere..this.asKey.."&"..value.."="..value
}

infix fun PropSQL.HAS_ALL_BITS(value: Int): Where {
    return newWhere..this.asKey.."&"..value.."="..value
}

infix fun String.HAS_ANY_BIT(value: Int): Where {
    return newWhere..this.asKey.."&"..value.."!= 0"
}

infix fun PropSQL.HAS_ANY_BIT(value: Int): Where {
    return newWhere..this.asKey.."&"..value.."!= 0"
}

infix fun PropSQL.ARRAY_ANY(value: Any): Where {
    return Where("ANY(${this.asColumn})=?") argValue value
}

//SELECT * from beltdev where (beltdev.tags)::jsonb @> '["一组"]'::jsonb;
infix fun PropSQL.JSONB_EXIST(value: String): Where {
    return Where("${this.modelFieldSQL.escapeSQL}::jsonb @> '$value'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, "teacher")
fun JSONB_ARRAY_EXIST_TEXT(prop: PropSQL, value: String): Where {
    return Where("${prop.modelFieldSQL.escapeSQL}::jsonb @> '${value.quoted}'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, 32)
fun JSONB_ARRAY_EXIST_NUMBER(prop: PropSQL, value: Number): Where {
    return Where("${prop.modelFieldSQL.escapeSQL}::jsonb @> '$value'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, listOf("teacher","student"))
fun JSONB_ARRAY_EXIST_TEXTS(prop: PropSQL, values: Iterable<String>): Where {
    val j = values.joinToString(",") { it.quoted }
    return Where("${prop.modelFieldSQL.escapeSQL}::jsonb @> '[$j]'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUES(Person::tags, listOf(1, 2))
fun JSONB_ARRAY_EXIST_NUMBERS(prop: PropSQL, values: Iterable<Number>): Where {
    val j = values.joinToString(",") { it.toString() }
    return Where("${prop.modelFieldSQL.escapeSQL}::jsonb @> '[$j]'::jsonb")
}