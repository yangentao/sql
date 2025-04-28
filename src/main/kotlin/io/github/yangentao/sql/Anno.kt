package io.github.yangentao.sql

import io.github.yangentao.reflect.getPropValue
import io.github.yangentao.sql.clause.ASC
import io.github.yangentao.sql.clause.EQ
import io.github.yangentao.sql.clause.EQUAL
import io.github.yangentao.sql.clause.FROM
import io.github.yangentao.sql.clause.INNER_JOIN
import io.github.yangentao.sql.clause.LIMIT
import io.github.yangentao.sql.clause.ON
import io.github.yangentao.sql.clause.ORDER_BY
import io.github.yangentao.sql.clause.SELECT
import io.github.yangentao.sql.clause.WHERE
import io.github.yangentao.sql.clause.query
import io.github.yangentao.sql.pool.namedConnection
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation

/**
 * Created by yangentao on 2016/12/14.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.TYPE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModelTable(
    val version: Int = 0
)

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModelView(
    val version: Int = 0
)

@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModelField(
    val primaryKey: Boolean = false,
    //0:false;  >0 : auto inc and set as  start value
    val autoInc: Int = 0,
    val unique: Boolean = false,
    val uniqueName: String = "",
    val index: Boolean = false,
    val notNull: Boolean = false,
    val defaultValue: String = "",
)

//DecimalFormat
//@Decimal(11, 2, "0.00")
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD, AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class Decimal(val precision: Int = 11, val scale: Int = 2, val pattern: String = "")

//------------------------------------------------

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class SQLFunction(val value: String)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class SQLProcedure(val value: String)

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class ParamIn

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class ParamOut

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class ParamInOut

//自动创建表
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class AutoCreateTable(val value: Boolean = true)

//外键
//@RefModel(Person::class,  "name")
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class ForeignKey(val foreignTable: KClass<out BaseModel>)

@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class RelatedList(val relationTable: KClass<out BaseModel>, val limit: Int = 200)

@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class RelatedOne(val keyProperty: String)

object RelationOneTarget {
    inline operator fun <reified T : BaseModel, reified R : BaseModel> getValue(thisRef: T, property: KProperty<*>): R? {
        return getValueX(T::class, R::class, thisRef, property)
    }

    fun <T : BaseModel, R : BaseModel> getValueX(thisClass: KClass<T>, targetClass: KClass<R>, thisRef: T, property: KProperty<*>): R? {
        val foreignModel: RelatedOne = property.findAnnotation<RelatedOne>() ?: error("No ForeignModel found. $property")
        val valueProp: KMutableProperty<*> = thisClass.propertiesHare.firstOrNull { it.name == foreignModel.keyProperty } ?: error("No foreign key property found. $property")
        val pKValue: Any = valueProp.getPropValue(thisRef) ?: return null
        val foreignPK = targetClass.primaryKeysHare.firstOrNull() ?: error("foreign table NO key defined, $property")
        return SELECT(targetClass.nameSQL.escapeSQL + ".*")
            .FROM(targetClass)
            .WHERE(foreignPK EQ pKValue)
            .LIMIT(1)
            .query(thisClass.namedConnection).one { orm(targetClass) }
    }
}

object RelationListTarget {

    inline operator fun <reified T : BaseModel, reified R : BaseModel> getValue(thisRef: T, property: KProperty<*>): List<R> {
        return getValueX(T::class, R::class, thisRef, property)
    }

    fun <T : BaseModel, R : BaseModel> getValueX(thisClass: KClass<T>, targetClass: KClass<R>, thisRef: T, property: KProperty<*>): List<R> {
        val relatedBy: RelatedList = property.findAnnotation<RelatedList>() ?: error("No Relatedby Annoation found! $property")
        val relatedClass: KClass<out BaseModel> = relatedBy.relationTable
        val relPkList: List<KMutableProperty<*>> = relatedClass.primaryKeysHare
        if (relPkList.size != 2) error("Relation table's Primary key size is NOT 2, $property")
        val relFirstPK: KMutableProperty<*> = relPkList[0]
        val relSecondPK: KMutableProperty<*> = relPkList[1]

        val firstClass: KClass<out BaseModel> = relFirstPK.findAnnotation<ForeignKey>()?.foreignTable ?: error("Relation Table has NO ForeignKey Annoation, $property")
        val secondClass: KClass<out BaseModel> = relSecondPK.findAnnotation<ForeignKey>()?.foreignTable ?: error("Relation Table has NO ForeignKey Annoation, $property")
        val thisPKValue: Any = thisClass.primaryKeysHare.first().getPropValue(thisRef)!!
        val targetPK = targetClass.primaryKeysHare.first()

        val relThisPK: KMutableProperty<*>
        val relTargetPK: KMutableProperty<*>
        if (firstClass == thisClass && secondClass == targetClass) {
            relThisPK = relFirstPK
            relTargetPK = relSecondPK
        } else if (firstClass == targetClass && secondClass == thisClass) {
            relThisPK = relSecondPK
            relTargetPK = relFirstPK
        } else error("Foreign Class misstake, $property")

        val node = SELECT(targetClass.nameSQL.escapeSQL + ".*")
            .FROM(targetClass INNER_JOIN relatedClass ON (targetPK.modelFieldSQL EQUAL relTargetPK.modelFieldSQL))
            .WHERE(relThisPK.modelFieldSQL EQ thisPKValue)
            .ORDER_BY(targetPK.ASC)
            .LIMIT(relatedBy.limit)
        return node.query(thisClass.namedConnection).list {orm(targetClass) }
    }

}
