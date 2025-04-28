package io.github.yangentao.reflect

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.kson.JsonFailed
import io.github.yangentao.kson.JsonResult
import java.sql.Connection
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Locale
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

interface Convertable {

}

open class TypeConverter {

}
val BadValue: JsonResult get() = JsonFailed("无效数据")
fun DatePattern.display(v: Any): String {
    return  dateDisplay(v, this.format)
}

fun dateDisplay(v: Any, format: String): String {
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
interface WithConnection {
    val connection: Connection
}
//mill seconds
class IntervalRun(private val interval: Long, initTime: Long = System.currentTimeMillis()) {
    private var lastTime: Long = initTime
    fun run(block: () -> Unit): Boolean {
        val tm = System.currentTimeMillis()
        if (tm - lastTime < interval) return false
        lastTime = tm
        block()
        return true
    }
}

@Suppress("RecursivePropertyAccessor")
val Throwable.rootError: Throwable
    get() {
        return this.cause?.rootError ?: this
    }
//ignore case equal
infix fun String?.ieq(other: String?): Boolean {
    return this.equals(other, ignoreCase = true)
}
@Suppress("UNCHECKED_CAST")
inline fun <reified E, reified T> Collection<E>.filterTyped(predicate: (E) -> Boolean): List<T> {
    return this.filter { predicate(it) && (it is T) } as List<T>
}
@Suppress("UNCHECKED_CAST")
class ClassProperty<T : Any>(val block: (KClass<*>) -> T) {

    operator fun getValue(thisRef: KClass<*>, property: KProperty<*>): T {
        val key = "$thisRef/$property"
        return map.getOrPut(key) { block(thisRef) } as T
    }

    companion object {
        val map = HashMap<String, Any>()
    }
}
operator fun StringBuilder.plusAssign(s: String) {
    this.append(s)
}

operator fun StringBuilder.plusAssign(ch: Char) {
    this.append(ch)
}

val String.quoted: String get() = if (this.startsWith("\"") && this.endsWith("\"")) this else "\"$this\"";

// a => 'a'
val String.quotedSingle: String
    get() {
        if (this.startsWith("'") && this.endsWith("'")) return this
        return "'$this'"
    }

internal infix fun String?.or(other: String): String {
    if (this.isNullOrEmpty()) return other
    return this
}

@Suppress("UNCHECKED_CAST")
internal inline fun <reified T> Collection<Any>.firstTyped(): T? {
    return this.firstOrNull { it is T } as? T
}

//12345.format(",###.##")
//12345.6789.format("0,000.00")
//@see DecimalFormat
internal fun Number.format(pattern: String): String {
    return if (pattern.isEmpty()) {
        this.toString()
    } else {
        DecimalFormat(pattern).format(this)
    }
}

internal fun Number.format(integers: Int, fractions: Int): String {
    val df = DecimalFormat()
    df.isGroupingUsed = false
    if (integers > 0) df.minimumIntegerDigits = integers
    if (fractions > 0) {
        df.minimumFractionDigits = fractions
        df.maximumFractionDigits = fractions
    }
    return df.format(this)
}