@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.ArgList
import io.github.yangentao.sql.escapeSQL
import io.github.yangentao.sql.fullNmeSQL
import io.github.yangentao.types.quoted

open class Where(condition: Any? = null, args: ArgList = emptyList()) : SQLExpress(condition, args)

class WhereAnd(ws: List<Where>) : Where() {
    init {
        this.addEach(ws.filter { it.isNotEmpty }, "AND") {
            if (it is WhereOr) this.parenthesed(it) else this..it
        }
    }
}

class WhereOr(ws: List<Where>) : Where() {
    init {
        this.addEach(ws.filter { it.isNotEmpty }, "OR")
    }
}

class WhereOper(left: Any, op: String, right: Any) : Where() {
    init {
        this..left..op..<right
    }
}

fun AND_ALL(vararg conditons: Where): Where {
    return WhereAnd(conditons.toList());
}

fun AND_ALL(conditons: List<Where>): Where {
    return WhereAnd(conditons);
}

fun OR_ALL(conditons: List<Where>): Where {
    return WhereOr(conditons)
}

infix fun Where.AND(other: Where): Where {
    return WhereAnd(listOf(this, other))
}

infix fun Where.OR(other: Where): Where {
    return WhereOr(listOf(this, other))
}

fun NOT(other: Where): Where {
    assert(other.isNotEmpty)
    return Where("NOT")..other
}

fun IS_NULL(exp: Any): Where {
    return Where(exp).."IS NULL"
}

fun IS_NOT_NULL(exp: Any): Where {
    return Where(exp).."IS NOT NULL"
}

fun EXISTS(exp: Any): Where {
    return Where("EXISTS")..exp
}

fun NOT_EXISTS(exp: Any): Where {
    return Where("NOT EXISTS")..exp
}

fun ANY_OF(exp: Any): Where {
    return Where("ANY(")..exp..")"
}

fun ALL_OF(exp: Any): Where {
    return Where("ALL(")..exp..")"
}

fun EQ_ALL_X(vararg cs: Pair<PropSQL, Any?>): Where {
    return EQ_ALL(cs.toList())
}

fun EQ_ALL(cs: List<Pair<PropSQL, Any?>>): Where {
    return AND_ALL(cs.map { it.first EQ it.second })
}

infix fun PropSQL.EQUAL(exp: Any): Where {
    return this.EQ(exp)
}

infix fun String.EQUAL(exp: Any): Where {
    return this.EQ(exp)
}

infix fun String.EQ(value: Any?): Where {
    if (value == null) return IS_NULL(this)
    return WhereOper(this, "=", value)
}

infix fun PropSQL.EQ(value: Any?): Where {
    if (value == null) return IS_NULL(this)
    return WhereOper(this, "=", value)
}

infix fun String.NE(value: Any?): Where {
    if (value == null) return IS_NOT_NULL(this)
    return WhereOper(this, "<>", value)
}

infix fun PropSQL.NE(value: Any?): Where {
    if (value == null) return IS_NOT_NULL(this)
    return WhereOper(this, "<>", value)
}

infix fun String.LE(value: Any): Where {
    return WhereOper(this, "<=", value)
}

infix fun PropSQL.LE(value: Any): Where {
    return WhereOper(this, "<=", value)
}

infix fun String.GE(value: Any): Where {
    return WhereOper(this, ">=", value)
}

infix fun PropSQL.GE(value: Any): Where {
    return WhereOper(this, ">=", value)
}

infix fun String.LT(value: Any): Where {
    return WhereOper(this, "<", value)
}

infix fun PropSQL.LT(value: Any): Where {
    return WhereOper(this, "<", value)
}

infix fun String.GT(value: Any): Where {
    return WhereOper(this, ">", value)
}

infix fun PropSQL.GT(value: Any): Where {
    return WhereOper(this, ">", value)
}

infix fun String.IN(exp: SQLExpress): Where {
    return Where(this).."IN("..exp..")"
}

infix fun PropSQL.IN(exp: SQLExpress): Where {
    return Where(this).."IN("..exp..")"
}

infix fun String.IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NULL(this)
    if (values.size == 1) return this.EQ(values.first())
    val w = Where(this).."IN"
    return w.parenthesed(values)
}

infix fun PropSQL.IN(values: Collection<Any>): Where {
    if (values.isEmpty()) return IS_NULL(this)
    if (values.size == 1) return this.EQ(values.first())
    val w = Where(this).."IN"
    return w.parenthesed(values)
}

fun String.BETWEEN(minValue: Any, maxValue: Any): Where {
    return Where(this).."BETWEEN"..minValue.."AND"..maxValue
}

fun PropSQL.BETWEEN(minValue: Any, maxValue: Any): Where {
    return Where(this).."BETWEEN"..minValue.."AND"..maxValue
}

infix fun String.LIKE(pattern: String): Where {
    return Where(this).."LIKE"..<pattern
}

infix fun PropSQL.LIKE(pattern: String): Where {
    return Where(this).."LIKE"..<pattern
}

infix fun String.ILIKE(pattern: String): Where {
    return Where(this).."ILIKE"..<pattern
}

infix fun PropSQL.ILIKE(pattern: String): Where {
    return Where(this).."ILIKE"..<pattern
}

infix fun String.GLOB(pattern: String): Where {
    return Where(this).."GLOB"..<pattern
}

infix fun String.SIMILAR_TO(pattern: String): Where {
    return Where(this).."SIMILAR TO"..<pattern
}

infix fun PropSQL.GLOB(pattern: String): Where {
    return Where(this).."GLOB"..<pattern
}

infix fun Any.HAS_ALL_BITS(value: Int): Where {
    return Where(this).."&"..value.."="..value
}

infix fun Any.HAS_ANY_BIT(value: Int): Where {
    return Where(this).."&"..value.."!="..0
}

infix fun PropSQL.ARRAY_ANY(value: Any): Where {
    val w = Where("ANY(")..this..")="..<value
    return w
}

//SELECT * from beltdev where (beltdev.tags)::jsonb @> '["一组"]'::jsonb;
infix fun PropSQL.JSONB_EXIST(value: String): Where {
    return Where("${this.fullNmeSQL.escapeSQL}::jsonb @> '$value'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, "teacher")
fun JSONB_ARRAY_EXIST_TEXT(prop: PropSQL, value: String): Where {
    return Where("${prop.fullNmeSQL.escapeSQL}::jsonb @> '${value.quoted}'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, 32)
fun JSONB_ARRAY_EXIST_NUMBER(prop: PropSQL, value: Number): Where {
    return Where("${prop.fullNmeSQL.escapeSQL}::jsonb @> '$value'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUE(Person::tags, listOf("teacher","student"))
fun JSONB_ARRAY_EXIST_TEXTS(prop: PropSQL, values: Iterable<String>): Where {
    val j = values.joinToString(",") { it.quoted }
    return Where("${prop.fullNmeSQL.escapeSQL}::jsonb @> '[$j]'::jsonb")
}

// JSONB_ARRAY_EXIST_VALUES(Person::tags, listOf(1, 2))
fun JSONB_ARRAY_EXIST_NUMBERS(prop: PropSQL, values: Iterable<Number>): Where {
    val j = values.joinToString(",") { it.toString() }
    return Where("${prop.fullNmeSQL.escapeSQL}::jsonb @> '[$j]'::jsonb")
}

