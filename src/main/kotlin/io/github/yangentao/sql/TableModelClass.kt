@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package io.github.yangentao.sql


import io.github.yangentao.kson.JsonResult
import io.github.yangentao.reflect.BadValue
import io.github.yangentao.reflect.Prop
import io.github.yangentao.reflect.PropInt
import io.github.yangentao.reflect.PropLong
import io.github.yangentao.reflect.WithConnection
import io.github.yangentao.reflect.decodeValue
import io.github.yangentao.reflect.ieq
import io.github.yangentao.reflect.returnClass
import io.github.yangentao.sql.clause.ALL
import io.github.yangentao.sql.clause.AND_ALL
import io.github.yangentao.sql.clause.ASC
import io.github.yangentao.sql.clause.COUNT
import io.github.yangentao.sql.clause.DELETE_FROM
import io.github.yangentao.sql.clause.DESC
import io.github.yangentao.sql.clause.DISTINCT
import io.github.yangentao.sql.clause.EQ
import io.github.yangentao.sql.clause.EQUAL
import io.github.yangentao.sql.clause.FROM
import io.github.yangentao.sql.clause.INNER_JOIN
import io.github.yangentao.sql.clause.INSERT_INTO
import io.github.yangentao.sql.clause.INSERT_INTO_VALUES
import io.github.yangentao.sql.clause.JOIN
import io.github.yangentao.sql.clause.LIMIT
import io.github.yangentao.sql.clause.LIMIT_OFFSET
import io.github.yangentao.sql.clause.LT
import io.github.yangentao.sql.clause.MAX
import io.github.yangentao.sql.clause.MIN
import io.github.yangentao.sql.clause.OFFSET
import io.github.yangentao.sql.clause.ON
import io.github.yangentao.sql.clause.ORDER_BY
import io.github.yangentao.sql.clause.ORDER_BY_LIST
import io.github.yangentao.sql.clause.PropSQL
import io.github.yangentao.sql.clause.SELECT
import io.github.yangentao.sql.clause.SET
import io.github.yangentao.sql.clause.SQLExpress
import io.github.yangentao.sql.clause.SQLNode
import io.github.yangentao.sql.clause.UNION
import io.github.yangentao.sql.clause.UPDATE
import io.github.yangentao.sql.clause.WHERE
import io.github.yangentao.sql.clause.WITH_RECURSIVE_SELECT
import io.github.yangentao.sql.clause.Where
import io.github.yangentao.sql.clause.asColumn
import io.github.yangentao.sql.clause.insert
import io.github.yangentao.sql.clause.query
import io.github.yangentao.sql.clause.update
import io.github.yangentao.sql.pool.namedConnection
import java.sql.Connection
import java.sql.ResultSet
import kotlin.collections.filterNotNull
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.findAnnotation

/**
 * Created by entaoyang@163.com on 2017/4/5.
 */

abstract class BaseModelClass<T : BaseModel> : WithConnection {
    @Suppress("UNCHECKED_CAST")
    val tableClass: KClass<T> = javaClass.enclosingClass.kotlin as KClass<T>
    final override val connection: Connection get() = tableClass.namedConnection
    val propsHare: List<KMutableProperty<*>>
        get() {
            return this.tableClass.propertiesHare
        }

    fun SQLExpress.query(): ResultSet {
        return query(connection)
    }

    fun SQLExpress.insert(): InsertResult {
        return insert(connection)
    }

    fun SQLExpress.update(): Int {
        return update(connection)
    }

    fun query(sa: String, args: List<Any?> = emptyList()): ResultSet {
        return connection.query(sa, args)
    }

    fun query(express: SQLNode): ResultSet {
        return express.query()
    }

    fun propertyByName(name: String): KMutableProperty<*>? {
        return propsHare.firstOrNull { it.fieldSQL ieq name }
    }

    fun decodeValue(field: String, value: Any): Any? {
        return propertyByName(field)?.decodeValue(value)
    }

    open fun createModel(map: Map<String, Any?>): T {
        return tableClass.createInstance().apply { model.putAll(map) }
    }

    fun keyEQ(value: Any): Where {
        val pks = tableClass.primaryKeysHare
        assert(pks.size == 1)
        return pks.first() EQ value
    }

    fun keysEQ(vararg values: Any): Where? {
        val pks = tableClass.primaryKeysHare
        assert(pks.isNotEmpty() && pks.size == values.size)
        val list: ArrayList<Where> = ArrayList()
        for (i in values.indices) {
            list.add(pks[i] EQ values[i])
        }
        return AND_ALL(list)
    }

    fun dumpTable() {
        SELECT().FROM(tableClass).query().dump()
    }

    fun exists(vararg ws: Where?): Boolean {
        return SELECT("1").FROM(tableClass).WHERE(*ws).LIMIT(1).query().exists()
    }

    fun countAll(vararg wheres: Where?): Int {
        return count("*", *wheres)
    }

    fun count(column: Any?, vararg wheres: Where?): Int {
        return SELECT(COUNT(column ?: "*")).FROM(tableClass).WHERE(wheres.filterNotNull()).query().one { intValue() } ?: 0
    }

    fun list(vararg wheres: Where?, block: SQLNode.() -> Unit = { }): List<T> {
        return list(wheres.filterNotNull(), block)
    }

    fun list(wheres: List<Where?>, block: SQLNode.() -> Unit = { }): List<T> {
        val exp = SELECT("*").FROM(tableClass).WHERE(wheres.filterNotNull())
        exp.block()
        return exp.query().list { orm(tableClass) }
    }

    fun listColumn(col: PropSQL, vararg wheres: Where?, block: SQLNode.() -> Unit = { }): ResultSet {
        return SELECT(DISTINCT, col).FROM(tableClass).WHERE(wheres.toList()).apply(block).query()
    }

    inline fun <reified R : Any> listColumnValue(col: KProperty<R>, vararg wheres: Where?, noinline block: SQLNode.() -> Unit = { }): List<R> {
        return listColumn(col, *wheres, block = block).list { valueAt(col) }
    }

    fun one(vararg wheres: Where?, orderBy: List<Any> = emptyList(), offset: Int? = null): T? {
        return one(wheres.filterNotNull(), orderBy = orderBy, offset = offset)
    }

    fun one(wheres: List<Where?>, orderBy: List<Any> = emptyList(), offset: Int? = null): T? {
        return SELECT("*").FROM(tableClass).WHERE(wheres.filterNotNull()).ORDER_BY_LIST(orderBy).LIMIT_OFFSET(1, offset).query().one { orm(tableClass) }
    }

    fun oneByKey(key: Any): T? {
        return this.one(keyEQ(key))
    }

    fun oneByKeys(vararg keys: Any): T? {
        return this.one(keysEQ(*keys))
    }

    fun oneColumn(col: Prop, vararg wheres: Where?, orderBy: List<Any> = emptyList()): ResultSet {
        return SELECT(col).FROM(tableClass).WHERE(wheres.toList()).ORDER_BY_LIST(orderBy).LIMIT(1).query()
    }

    inline fun <reified E : Any> oneColumnValue(col: KProperty<E?>, vararg wheres: Where?, orderBy: List<Any> = emptyList()): E? {
        return oneColumn(col, *wheres, orderBy = orderBy).one { valueAt(col, 1) }
    }

    inline fun <reified R : Any> oneColumnByKey(col: KProperty<R?>, key: Any): R? {
        return oneColumnValue(col, keyEQ(key))
    }

    inline fun <reified T> maxColumn(col: KProperty<T>, vararg ws: Where?): T? {
        return SELECT(MAX(col)).FROM(tableClass).WHERE(*ws).query().one { valueAt() }
    }

    inline fun <reified T> minColumn(col: KProperty<T>, vararg ws: Where?): T? {
        return SELECT(MIN(col)).FROM(tableClass).WHERE(*ws).query().one { valueAt() }
    }

    fun maxRowBy(p: Prop, vararg ws: Where?): T? {
        return one(ws.filterNotNull(), orderBy = listOf(p.DESC))
    }

    fun minRowBy(p: Prop, vararg ws: Where?): T? {
        return one(ws.filterNotNull(), orderBy = listOf(p.ASC))
    }
}

abstract class ViewModelClass<T : ViewModel> : BaseModelClass<T>() {

    init {
        migrate()
    }

    abstract fun onCreateView(): SQLNode?

    fun migrate() {
        val viewName = tableClass.nameSQL
        val b = connection.viewExists(viewName)
        if (!b) {
            val node = onCreateView()
            if (node != null) createView(node)
        }
    }

    fun createView(query: SQLNode) {
        val viewName = tableClass.nameSQL
        val a = query
        connection.exec("CREATE VIEW $viewName AS ${a.sql}", a.arguments)
    }
}

open class TableModelClass<T : TableModel> : BaseModelClass<T>() {


    init {
        TableMigrater(connection, tableClass)
    }

    fun upsert(vararg ps: Pair<Any, Any?>, conflict: Conflicts = Conflicts.Update): InsertResult {
        return connection.upsert(tableClass.nameSQL, ps.map { it.first.asColumn to it.second }, tableClass.primaryKeysHare.map { it.fieldSQL }, conflict = conflict)
    }

    fun upsert(conflict: Conflicts = Conflicts.Update, block: (T) -> Unit): ModelInsertResult<T> {
        val m = tableClass.createInstance()
        block(m)
        val r = m.upsert(conflict = conflict)
        return ModelInsertResult(m, r)
    }

    fun insertInto(vararg cols: Any, block: InsertIntoValues.() -> Unit): InsertResult {
        val valueList: ArrayList<List<Any?>> = ArrayList()
        InsertIntoValues(valueList).apply(block)
        return INSERT_INTO_VALUES(tableClass, cols.toList(), valueList).insert(tableClass.namedConnection)
    }

    fun insert(block: (T) -> Unit): ModelInsertResult<T> {
        val m = tableClass.createInstance()
        block(m)
        val r = m.insert()
        return ModelInsertResult(m, r)
    }

    fun insert(vararg ps: Pair<Any, Any?>): InsertResult {
        return INSERT_INTO(tableClass, ps.toList()).insert()
    }

    fun delete(vararg conditions: Where?): Int {
        return delete(conditions.toList())
    }

    fun delete(conditions: List<Where?>): Int {
        return DELETE_FROM(tableClass).WHERE(AND_ALL(conditions)).update()
    }

    fun deleteByKey(key: Any): Int {
        return delete(keyEQ(key))
    }

    fun updateX(express: SQLExpress): Int {
        return express.update()
    }

    fun update(w: Where?, vararg ps: Pair<Any, Any?>): Int {
        return update(w, ps.toList())
    }

    fun update(w: Where?, ps: List<Pair<Any, Any?>>): Int {
        return UPDATE(tableClass).SET(ps.toList()).WHERE(w).update()
    }

    fun update(w: Where?, map: Map<Any, Any?>): Int {
        return update(w, map.entries.map { it.key to it.value })
    }

    fun updateByKey(key: Any, vararg ps: Pair<Any, Any?>): Int {
        return updateByKey(key, ps.toList())
    }

    fun updateByKey(key: Any, ps: List<Pair<Any, Any?>>): Int {
        return update(keyEQ(key), ps)
    }

    fun updateByKey(key: Any, map: Map<String, Any?>): Int {
        return updateByKey(key, map.entries.map { it.key to it.value })
    }

    fun update(key: Any, column: String, value: String, allowColumns: Set<String>): JsonResult {
        if (column !in allowColumns) return BadValue
        val g = oneByKey(key) ?: return BadValue
        val v = this.decodeValue(column, value) ?: return BadValue
        val n = g.updateByKey(column to v)
        return JsonResult(ok = n > 0)
    }

    //通过关联表, 来查找自己的类型
    //table: user, group, member(group.id, user.id)
    //user.relateTo(member::class, member::groupId EQ 100)
    fun relatedBy(relateTable: KClass<*>, vararg ws: Where?, block: SQLNode.() -> Unit = { }): List<T> {
        val thisPK = tableClass.primaryKeysHare.first()
        val relatePK = relateTable.primaryKeysHare.first {
            val fk = it.findAnnotation<ForeignKey>()
            fk != null && fk.foreignTable == tableClass
        }
        val exp = SELECT(this.ALL)
            .FROM(tableClass INNER_JOIN relateTable ON (thisPK EQUAL relatePK))
            .WHERE(ws.toList())
        exp.block()
        return exp.query().list { orm(tableClass) }

    }

    //通过关联表, 来查找自己的类型
    //table: user, group, member(group.id, user.id)
    //user.relateTo(member::class, member::groupId EQ 100)
    fun relatedByTarget(relateTable: KClass<*>, targetKey: Any, block: SQLNode.() -> Unit = { }): List<T> {
        val thisPKs = tableClass.primaryKeysHare
        assert(thisPKs.size == 1)
        val thisPK = tableClass.primaryKeysHare.first()
        val relPKs = relateTable.primaryKeysHare
        assert(relPKs.size == 2)
        val relThisPK = relPKs.firstOrNull {
            it.findAnnotation<ForeignKey>()?.foreignTable == tableClass
        } ?: error("No ForeignKey found, $relateTable")
        val relTargetPK = relPKs.firstOrNull {
            it.findAnnotation<ForeignKey>()?.foreignTable != tableClass
        } ?: error("No ForeignKey found, $relateTable")
        val exp = SELECT(this.ALL)
            .FROM(tableClass INNER_JOIN relateTable ON (thisPK EQUAL relThisPK))
            .WHERE(relTargetPK EQ targetKey)
        exp.block()
        return exp.query().list { orm(tableClass) }

    }

    fun limitTable(maxRow: Int) {
        if (maxRow <= 0) {
            return
        }
        val pks = tableClass.primaryKeysHare
        assert(pks.size == 1)
        val pk = pks.first()
        assert(pk.returnType.classifier == Int::class || pk.returnType.classifier == Long::class)
        val n: Long = SELECT(pk).FROM(tableClass).ORDER_BY(pk.DESC).LIMIT(1).OFFSET(maxRow).query().one { longValue() } ?: return
        this.delete(pk LT n)
    }

    //growingColumn是带索引的, 增长的列, 比如 自增主键, 或时间戳(整型, 字符串均可)
    fun limitTableByGrowingColumn(growingColumn: KProperty<*>, maxRow: Int) {
        if (maxRow <= 0) {
            return
        }
        val n: Any = SELECT(growingColumn).FROM(tableClass).ORDER_BY(growingColumn.DESC).LIMIT(1).OFFSET(maxRow).query().one { valueAt(growingColumn.returnClass) } ?: return
        this.delete(growingColumn LT n)
    }
}

data class ModelInsertResult<T>(val model: T, val result: InsertResult) {
    val success: Boolean get() = result.success
    val key: Long get() = result.key
}

//上级
fun <T : TableModel> BaseModelClass<T>.listParents(parentCol: PropSQL, idValue: Any, includeCurrent: Boolean = true): List<T> {
    val keyList = this.tableClass.primaryKeysHare
    assert(keyList.size == 1)
    val keyCol: Prop = this.tableClass.primaryKeysHare.first()
    val propList = tableClass.propertiesHare
    val ls: List<T> = WITH_RECURSIVE_SELECT("rc") {
        val a = SELECT(propList).FROM(tableClass).WHERE(keyCol EQ idValue)
        val b = SELECT(propList).FROM(tableClass JOIN "rc" ON (keyCol EQUAL "rc.${parentCol.fieldSQL}"))
        a UNION b
    }.query(tableClass.namedConnection).list { orm(tableClass) }
    if (includeCurrent) return ls
    return ls.toMutableList().also { e -> e.removeIf { it[keyCol] == idValue } }
}

//上级
fun <T : Any> BaseModelClass<*>.listParentKey(parentCol: KProperty<T>, idValue: T, includeCurrent: Boolean = true): List<T> {
    val keyList = this.tableClass.primaryKeysHare
    assert(keyList.size == 1)
    val keyCol: Prop = this.tableClass.primaryKeysHare.first()

    val ls: List<T> = WITH_RECURSIVE_SELECT("rc") {
        val a = SELECT(keyCol, parentCol).FROM(tableClass).WHERE(keyCol EQ idValue)
        val b = SELECT(keyCol, parentCol).FROM(tableClass JOIN "rc" ON (keyCol EQUAL "rc.${parentCol.fieldSQL}"))
        a UNION b
    }.query(tableClass.namedConnection).list { valueAt(keyCol.returnClass, 1) }
    if (includeCurrent) return ls
    return ls.toMutableList().also { it.remove(idValue) }
}

fun BaseModelClass<*>.listParentKeyLong(parentCol: PropLong, idValue: Long, includeCurrent: Boolean = true): List<Long> {
    return listParentKey(parentCol, idValue, includeCurrent)
}

fun BaseModelClass<*>.listParentKeyInt(parentCol: PropInt, idValue: Int, includeCurrent: Boolean = true): List<Int> {
    return listParentKey(parentCol, idValue, includeCurrent)
}
