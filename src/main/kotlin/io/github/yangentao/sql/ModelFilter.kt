package io.github.yangentao.sql

import io.github.yangentao.sql.clause.AND_ALL
import io.github.yangentao.sql.clause.DELETE_FROM
import io.github.yangentao.sql.clause.FROM
import io.github.yangentao.sql.clause.LIMIT
import io.github.yangentao.sql.clause.LIMIT_OFFSET
import io.github.yangentao.sql.clause.ORDER_BY_LIST
import io.github.yangentao.sql.clause.SELECT
import io.github.yangentao.sql.clause.SELECT_LIST
import io.github.yangentao.sql.clause.SET
import io.github.yangentao.sql.clause.UPDATE
import io.github.yangentao.sql.clause.WHERE
import io.github.yangentao.sql.clause.Where
import io.github.yangentao.sql.clause.query
import io.github.yangentao.sql.clause.update
import io.github.yangentao.sql.pool.namedConnection
import kotlin.reflect.KClass

class ModelFilterSelectOrderByLimit(val table: KClass<*>, val wheres: List<Where>, val selects: List<Any>, val orderBys: List<String>, val limit: Int, val offset: Int) {
    fun <R> list(blockResult: ResultRow.() -> R?): List<R> {
        val node = SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys).LIMIT_OFFSET(limit, offset)
        return node.query(table.namedConnection).list(blockResult)
    }

    fun <R> one(blockResult: ResultRow.() -> R?): R? {
        val node = SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys).LIMIT_OFFSET(limit, offset)
        return node.query(table.namedConnection).one(blockResult)
    }
}

class ModelFilterSelectOrderBy(val table: KClass<*>, val wheres: List<Where>, val selects: List<Any>, val orderBys: List<String>) {
    fun <R> one(block: ResultRow.() -> R?): R? {
        return SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys).LIMIT(1).query(table.namedConnection).one(block)
    }

    fun <R> list(limit: Int? = null, offset: Int? = null, blockResult: ResultRow.() -> R?): List<R> {
        val node = SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys)
        if (limit != null) {
            node.LIMIT_OFFSET(limit, offset ?: 0)
        }
        return node.query(table.namedConnection).list(blockResult)
    }

    fun limit(limit: Int, offset: Int = 0): ModelFilterSelectOrderByLimit {
        return ModelFilterSelectOrderByLimit(table, wheres, selects, orderBys, limit, offset)
    }
}

class ModelFilterSelect(val table: KClass<*>, val wheres: List<Where>, val selects: List<Any>) {
    // first row, first column
    inline fun <reified R> oneValue(vararg orderBys: String): R? {
        return SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys.toList()).LIMIT(1).query(table.namedConnection).one { valueAt(1) }
    }

    fun <R> one(vararg orderBys: String, block: ResultRow.() -> R?): R? {
        return SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBys.toList()).LIMIT(1).query(table.namedConnection).one(block)
    }

    fun <R> list(vararg orderBy: String, limit: Int? = null, offset: Int? = null, blockResult: ResultRow.() -> R?): List<R> {
        val node = SELECT_LIST(selects).FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBy.toList())
        if (limit != null) {
            node.LIMIT_OFFSET(limit, offset ?: 0)
        }
        return node.query(table.namedConnection).list(blockResult)
    }

    fun orderBy(vararg orderBys: String): ModelFilterSelectOrderBy {
        return ModelFilterSelectOrderBy(table, wheres, selects, orderBys.toList())
    }

}

class ModelFilter<T : BaseModel>(val table: KClass<T>, val wheres: List<Where>) {
    fun exists(): Boolean {
        return SELECT("1").FROM(table).WHERE(wheres).LIMIT(1).query(table.namedConnection).exists()
    }

    fun one(vararg orderBy: String): T? {
        return SELECT("*").FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBy.toList()).LIMIT(1).query(table.namedConnection).one { orm(table) }
    }

    fun list(vararg orderBy: String, limit: Int? = null, offset: Int? = null): List<T> {
        val exp = SELECT("*").FROM(table).WHERE(wheres).ORDER_BY_LIST(orderBy.toList())
        if (limit != null) {
            exp.LIMIT_OFFSET(limit, offset ?: 0)
        }
        return exp.query(table.namedConnection).list { orm(table) }
    }

    fun delete(): Int {
        return DELETE_FROM(table).WHERE(AND_ALL(wheres)).update(table.namedConnection)
    }

    fun update(vararg pairs: Pair<Any, Any?>): Int {
        return UPDATE(table).SET(pairs.toList()).WHERE(wheres).update(table.namedConnection)
    }

    fun select(vararg exps: Any): ModelFilterSelect {
        return ModelFilterSelect(table, wheres, exps.toList())
    }
}

fun <T : BaseModel> BaseModelClass<T>.filter(vararg ws: Where?): ModelFilter<T> {
    return ModelFilter(this.tableClass, ws.toList().filterNotNull())
}
