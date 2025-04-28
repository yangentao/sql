@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.reflect.KotClass
import io.github.yangentao.sql.BaseModelClass

class SQLNode(clause: String? = null) : SQLExpress(clause)

private val newNode: SQLNode get() = SQLNode()

//var sql = """
//       WITH RECURSIVE cte(id, pid, name) AS (
//           SELECT id, pid ,name
//           FROM dept
//           WHERE  name = 'dev'
//           UNION
//           SELECT a.id, a.pid , a.name
//           FROM dept as a
//           JOIN cte ON a.id = cte.pid
//       )
//       SELECT id, pid, name FROM cte ;
//   """.trimIndent()
fun WITH_RECURSIVE(name: String, vararg cols: Any, block: () -> SQLNode): SQLNode {
    if (cols.isEmpty()) {
        return newNode.."WITH RECURSIVE $name AS ("..block()..")"
    }
    return newNode.."WITH RECURSIVE $name("..cols..") AS ("..block()..")"
}

fun WITH_RECURSIVE_SELECT(name: String, vararg cols: Any, block: () -> SQLNode): SQLNode {
    if (cols.isEmpty()) {
        return newNode.."WITH RECURSIVE $name AS ("..block()..") SELECT * FROM $name"
    }
    return newNode.."WITH RECURSIVE $name("..cols..") AS ("..block()..") SELECT "..cols.."FROM $name"
}

infix fun String.AS(right: Any): SQLNode {
    return newNode..this.asKey.."AS"..right.asKey
}

infix fun PropSQL.AS(right: Any): SQLNode {
    return newNode..this.."AS"..right.asKey
}

infix fun BaseModelClass<*>.AS(right: Any): SQLNode {
    return this.tableClass.AS(right.asKey)
}

infix fun KotClass.AS(right: Any): SQLNode {
    return newNode..this.."AS"..right.asKey
}

infix fun SQLNode.AS(right: Any): SQLNode {
    return this.."AS"..right.asKey
}

val RECURSIVE: String get() = "RECURSIVE "
fun WITH(vararg exps: Any): SQLNode {
    return newNode.."WITH"..exps
}

fun SQLNode.SELECT(vararg exps: Any): SQLNode {
    return this.._SELECT_LIST(exps.toList())
}

fun SQLNode.SELECT_LIST(exps: List<Any>): SQLNode {
    return this.._SELECT_LIST(exps)
}

fun SELECT(vararg exps: Any): SQLNode {
    return _SELECT_LIST(exps.toList())
}

fun SELECT_LIST(exps: List<Any>): SQLNode {
    return _SELECT_LIST(exps)
}

private fun _SELECT(vararg exps: Any): SQLNode {
    return _SELECT_LIST(exps.toList())
}

private fun _SELECT_LIST(exps: List<Any>): SQLNode {
    if (exps.isEmpty()) return newNode.."SELECT *"
    val first = exps.first()
    when (first) {
        is DistinctExp -> return newNode.."SELECT"..first..exps.slice(1..<exps.size)
        is DistinctOnExp ->
            return if (exps.size == 1) {
                newNode.."SELECT"..first.." *"
            } else {
                newNode.."SELECT"..first..exps.slice(1..<exps.size)
            }

        else -> return newNode.."SELECT"..exps
    }
}

inline fun <reified T : Any> SQLNode.FROM(): SQLNode {
    return FROM(T::class)
}

fun SQLNode.FROM(vararg exps: Any): SQLNode {
    return FROM_LIST(exps.toList())
}

fun SQLNode.FROM_LIST(exps: List<Any>): SQLNode {
    return this.."FROM "..exps
}

private fun SQLNode.WHERE_(condition: Where?): SQLNode {
    if (condition == null || condition.isEmpty) return this
    return this.."WHERE"..condition
}

fun SQLNode.WHERE(vararg conditions: Where?): SQLNode {
    return WHERE(conditions.toList())
}

fun SQLNode.WHERE(conditions: List<Where?>): SQLNode {
    return WHERE_(AND_ALL(conditions))
}

fun SQLNode.ORDER_BY(vararg exps: Any): SQLNode {
    return this.ORDER_BY_LIST(exps.toList())
}

fun SQLNode.ORDER_BY_LIST(exps: List<Any>): SQLNode {
    if (exps.isEmpty()) return this
    return this.."ORDER BY"..exps
}

fun SQLNode.GROUP_BY(vararg exps: Any): SQLNode {
    return GROUP_BY_LIST(exps.toList())
}

fun SQLNode.GROUP_BY_LIST(exps: List<Any>): SQLNode {
    if (exps.isEmpty()) return this
    return this.."GROUP BY"..exps
}

fun SQLNode.CUBE(vararg exps: Any): SQLNode {
    return this.."CUBE"..exps
}

fun SQLNode.ROLLUP(vararg exps: Any): SQLNode {
    return this.."ROLLUP"..exps
}

fun SQLNode.GROUPING_SETS(vararg exps: List<Any>): SQLNode {
    this.."GROUPING SETS ("
    this.addList(exps.toList()) { e, ls ->
        e.."("
        e.addList(ls) { e2, item ->
            e2..item.asKey
        }
        e..")"
    }
    this..")"
    return this
}

fun SQLNode.HAVING(condition: Where): SQLNode {
    return this.."HAVING"..condition
}

fun SQLNode.LIMIT(size: Number, offset: Number? = null): SQLNode {
    if (offset == null) return this.."LIMIT"..size
    return this.."LIMIT"..size.."OFFSET"..offset
}

fun SQLNode.OFFSET(offset: Number): SQLNode {
    return this.."OFFSET"..offset
}

fun SQLNode.LIMIT_OFFSET(size: Number, offset: Number?): SQLNode {
    if (offset == null) return this.."LIMIT"..size
    return this.."LIMIT"..size.."OFFSET"..offset
}