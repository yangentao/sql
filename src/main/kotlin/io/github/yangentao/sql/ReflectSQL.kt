package io.github.yangentao.sql

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.utils.ClassProperty
import io.github.yangentao.types.declaredMemberPropertiesSorted
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.findAnnotation

val <T : Any>  KClass<T>.propertiesModel: List<ModelProperty> by ClassProperty { cls ->
    cls.declaredMemberPropertiesSorted.mapNotNull { p ->
        if (p is KMutableProperty<*>) {
            p.findAnnotation<ModelField>()?.let { ModelProperty(p as KMutableProperty<*>, it) }
        } else null
    }
}
val <T : Any>  KClass<T>.propertiesHare: List<KMutableProperty<*>> by ClassProperty { cls ->
    cls.propertiesModel.map { it.property }
}
val <T : Any>  KClass<T>.primaryKeysHare: List<KMutableProperty<*>> by ClassProperty { cls ->
    cls.propertiesModel.filter { it.annotation.primaryKey }.map { it.property }
}

data class ModelProperty(val property: KMutableProperty<*>, val annotation: ModelField)