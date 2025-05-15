package io.github.yangentao.sql

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.kson.JsonFailed
import io.github.yangentao.kson.JsonResult
import io.github.yangentao.types.DateTime
import io.github.yangentao.types.format
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty



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

