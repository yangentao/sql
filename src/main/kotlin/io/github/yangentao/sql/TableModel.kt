@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package io.github.yangentao.sql


import io.github.yangentao.anno.Exclude
import io.github.yangentao.kson.Kson
import io.github.yangentao.sql.clause.AND_ALL
import io.github.yangentao.sql.clause.DELETE_FROM
import io.github.yangentao.sql.clause.EQ
import io.github.yangentao.sql.clause.FROM
import io.github.yangentao.sql.clause.INSERT_INTO
import io.github.yangentao.sql.clause.LIMIT
import io.github.yangentao.sql.clause.SELECT
import io.github.yangentao.sql.clause.SET
import io.github.yangentao.sql.clause.SQLExpress
import io.github.yangentao.sql.clause.UPDATE
import io.github.yangentao.sql.clause.WHERE
import io.github.yangentao.sql.clause.Where
import io.github.yangentao.sql.clause.insert
import io.github.yangentao.sql.clause.query
import io.github.yangentao.sql.clause.update
import io.github.yangentao.sql.pool.namedConnection
import io.github.yangentao.types.Prop
import io.github.yangentao.types.getPropValue
import io.github.yangentao.types.setPropValue
import java.sql.Connection
import java.sql.ResultSet
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty

/**
 * Created by entaoyang@163.com on 2017/3/31.
 */

abstract class BaseModel : WithConnection {
    @Exclude
    val model: OrmMap = OrmMap()

    @Exclude
    override val connection: Connection get() = this::class.namedConnection

    fun SQLExpress.query(): ResultSet {
        return query(connection)
    }

    fun SQLExpress.insert(): InsertResult {
        return insert(connection)
    }

    fun SQLExpress.update(): Int {
        return update(connection)
    }

    operator fun get(key: String): Any? {
        return model[key]
    }

    operator fun get(p: Prop): Any? {
        return model[p]
    }

    fun hasProp(p: KProperty<*>): Boolean {
        return hasProp(p.fieldSQL)
    }

    fun hasProp(key: String): Boolean {
        return model.containsKey(key) || model.containsKey(key.lowercase())
    }

    fun removeProperty(p: KProperty<*>) {
        model.removeProperty(p)
    }

    fun existByKey(): Boolean {
        val w = this.primaryKeyCondition ?: throw IllegalArgumentException("必须设置主键")
        val cls = this::class
        return SELECT("1").FROM(cls).WHERE(w).LIMIT(1).query().exists()
    }

    @Exclude
    val primaryKeyCondition: Where?
        get() {
            val ls = ArrayList<Where>()
            this::class.primaryKeysHare.forEach {
                if (hasProp(it)) {
                    ls.add(it EQ it.getPropValue(this))
                } else error("NO PrimaryKey Value ")
            }
            return if (ls.isEmpty()) null else AND_ALL(ls)
        }

    //仅包含有值的列, modMap中出现
    @Exclude
    val propertiesExists: List<KMutableProperty<*>>
        get() {
            return this::class.propertiesHare.filter { model.hasProp(it) }
        }

    override fun toString(): String {
        return Kson.toKson(model).toString()
    }
}

abstract class ViewModel : BaseModel() {

}

abstract class TableModel : BaseModel() {

    fun deleteByKey(): Boolean {
        val w = this.primaryKeyCondition ?: return false
        return DELETE_FROM(this::class).WHERE(w).update() > 0
    }

    fun insert(): InsertResult {
        val colList = propertiesExists.map { it to model[it] }
        val aiList = this::class.propertiesModel.filter { it.annotation.autoInc > 0 }
        val ret = INSERT_INTO(this::class, colList).insert()
        if (ret.count > 0L) {
            for (p in aiList) {
                val v = ret.keyOf(p.property)
                if (v != null) p.property.setPropValue(this, v)
            }

        }
        return ret
    }

    fun upsert(conflict: Conflicts = Conflicts.Update): InsertResult {
        return connection.upsert(this, conflict = conflict)
    }

    //update exist properties
    fun updateProps(vararg ps: KMutableProperty<*>): Int {
        return updateProps(ps.toList())
    }

    //update exist properties
    fun updateProps(ps: List<KMutableProperty<*>>): Int {
        if (ps.isNotEmpty()) {
            val kvs: List<Pair<Any, Any?>> = ps.map { it to it.getPropValue(this) }
            return updateByKey(kvs)
        }
        val allProp = this.propertiesExists
        val pk = this::class.primaryKeysHare
        val leftProps = allProp.filter { it !in pk }
        if (leftProps.isEmpty()) return 0
        return updateProps(leftProps.map { it })
    }

    fun updateByKey(vararg kvs: Pair<Any, Any?>): Int {
        return updateByKey(kvs.toList())
    }

    fun updateByKey(kvs: List<Pair<Any, Any?>>): Int {
        if (kvs.isEmpty()) return 0
        return UPDATE(this::class).SET(kvs).WHERE(primaryKeyCondition!!).update()
    }

}

fun <T : TableModel> T.update(block: (T) -> Unit): Int {
    val ls = this.model.gather {
        block(this)
    }
    if (ls.isNotEmpty()) {
        return this.updateProps(ls)
    }
    return 0
}







