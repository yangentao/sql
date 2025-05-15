package io.github.yangentao.sql

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.anno.SerialMe
import io.github.yangentao.anno.isHidden
import io.github.yangentao.anno.userName
import io.github.yangentao.kson.*
import io.github.yangentao.types.*
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation

val <T : Any>  KClass<T>.serialProperties: List<KProperty<*>> by ClassProperty { cls ->
    cls.declaredMemberPropertiesSorted.filter { p ->
        p.hasAnnotation<SerialMe>()
    }
}

fun <T : BaseModel> T.toJson(
    only: List<Prop> = emptyList(),
    includes: List<Prop> = emptyList(),
    excludes: List<Prop> = emptyList(),
    attrs: List<Pair<String, Any?>> = emptyList()
): KsonObject {
    val ls = ArrayList<Prop>()
    if (only.isNotEmpty()) {
        ls.addAll(only)
    } else {
        ls.addAll(this::class.propertiesHare.filter { !it.isHidden })
    }
    ls.addAll(this::class.serialProperties)
    if (includes.isNotEmpty()) ls.addAll(includes)
    if (excludes.isNotEmpty()) {
        val set = excludes.map { it.userName }
        ls.removeIf { item -> item.userName in set }
    }

    val jo = KsonObject()
    for (p in ls) {
        val v = p.getPropValue(this);
        jo.putAny(p.userName, pval(p, v))
    }
    for ((k, v) in attrs) {
        jo.putAny(k, v)
    }
    return jo
}

private fun pval(p: KProperty<*>, value: Any?): Any? {
    val v = value ?: return null
    p.findAnnotation<DatePattern>()?.let { return it.display(v) }
    if (v is Number) {
        val s = p.encodeToString(v)
        return BigDecimal(s)
    }

    return v
}

inline fun <reified T : BaseModel> Collection<T>.jsonArrayClient(includes: List<Prop> = emptyList()): KsonArray {
    return ksonArray(this) { it.toJson(includes = includes) }
}

fun <T : BaseModel> JsonResult.dataListModel(list: List<T>, includes: List<Prop> = emptyList()): JsonResult {
    dataList(list) { it.toJson(includes = includes) }
    return this
}

fun BaseModel.jsonResult(includes: List<Prop> = emptyList(), excludes: List<Prop> = emptyList(), attrs: List<Pair<String, Any?>> = emptyList()): JsonResult {
    return JsonResult(ok = true, data = this.toJson(includes = includes, excludes = excludes, attrs = attrs))
}

fun <T : BaseModel> List<T>.jsonResult(total: Int = 0, offset: Int? = null, includes: List<Prop> = emptyList(), excludes: List<Prop> = emptyList(), attrs: List<Pair<String, Any?>> = emptyList()): JsonResult {
    return JsonSuccess(attrs = listOf("total" to total.greatEqual(this.size), "offset" to (offset ?: 0))).dataList(this) {
        it.toJson(includes = includes, excludes = excludes, attrs = attrs)
    }
}
