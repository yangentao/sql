package io.github.yangentao.sql.utils

import io.github.yangentao.kson.JsonFailed
import io.github.yangentao.kson.JsonResult
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

object StateVal {
    const val NORMAL: Int = 0
    const val DISABLED: Int = 1
    const val DELETED: Int = 2
}

@Suppress("UNCHECKED_CAST")
internal class ClassProperty<T : Any>(val block: (KClass<*>) -> T) {

    operator fun getValue(thisRef: KClass<*>, property: KProperty<*>): T {
        val key = "$thisRef/$property"
        return map.getOrPut(key) { block(thisRef) } as T
    }

    companion object {
        val map = HashMap<String, Any>()
    }
}

internal val BadValue: JsonResult get() = JsonFailed("无效数据")