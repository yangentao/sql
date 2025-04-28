package io.github.yangentao.sql

import io.github.yangentao.sql.clause.*
import io.github.yangentao.sql.pool.namedConnection
import io.github.yangentao.types.getPropValue
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation

/**
 * Created by yangentao on 2016/12/14.
 */

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
        return node.query(thisClass.namedConnection).list { orm(targetClass) }
    }

}
