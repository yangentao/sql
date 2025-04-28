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

internal operator fun StringBuilder.plusAssign(s: String) {
    this.append(s)
}

internal operator fun StringBuilder.plusAssign(ch: Char) {
    this.append(ch)
}

internal val String.quoted: String get() = if (this.startsWith("\"") && this.endsWith("\"")) this else "\"$this\"";

// a => 'a'
internal val String.quotedSingle: String
    get() {
        if (this.startsWith("'") && this.endsWith("'")) return this
        return "'$this'"
    }

internal infix fun String?.or(other: String): String {
    if (this.isNullOrEmpty()) return other
    return this
}

@Suppress("UNCHECKED_CAST")
internal inline fun <reified E, reified T> Collection<E>.filterTyped(predicate: (E) -> Boolean): List<T> {
    return this.filter { predicate(it) && (it is T) } as List<T>
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

//ignore case equal
internal infix fun String?.ieq(other: String?): Boolean {
    return this.equals(other, ignoreCase = true)
}

@Suppress("RecursivePropertyAccessor")
internal val Throwable.rootError: Throwable
    get() {
        return this.cause?.rootError ?: this
    }

internal val BadValue: JsonResult get() = JsonFailed("无效数据")
internal fun DatePattern.display(v: Any): String {
    return dateDisplay(v, this.format)
}

internal fun dateDisplay(v: Any, format: String): String {
    //java.util.Date包含java.sql.Date和Timestamp,Time
    return when (v) {
        is java.util.Date -> SimpleDateFormat(format, Locale.getDefault()).format(v)
        is Long -> SimpleDateFormat(format, Locale.getDefault()).format(java.util.Date(v))
        is DateTime -> v.format(format)
        is LocalDate -> v.format(format)
        is LocalDateTime -> v.format(format)
        is LocalTime -> v.format(format)
        else -> v.toString()
    }
}

internal inline fun <reified T : Comparable<T>> T.greatEqual(v: T): T {
    if (this < v) return v
    return this
}

