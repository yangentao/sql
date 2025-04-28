@file:Suppress("unused")

package io.github.yangentao.reflect

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.anno.NumberPattern
import io.github.yangentao.anno.SepChar
import io.github.yangentao.anno.StringFormat
import io.github.yangentao.kson.KsonArray
import io.github.yangentao.kson.KsonBool
import io.github.yangentao.kson.KsonNull
import io.github.yangentao.kson.KsonNum
import io.github.yangentao.kson.KsonObject
import io.github.yangentao.kson.KsonString
import io.github.yangentao.sql.Decimal
import java.math.BigDecimal
import java.net.URI
import java.net.URL
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Date
import java.util.UUID
import kotlin.collections.iterator
import kotlin.reflect.*

fun KType.encodeToString(value: Any?): String? {
    return encodeValueToString(value, this.annotations)
}

fun KParameter.encodeToString(value: Any?): String? {
    return encodeValueToString(value, this.annotations)
}

fun KProperty<*>.encodeToString(value: Any?): String? {
    return encodeValueToString(value, this.annotations)
}

fun KProperty0<*>.encodeToString(): String? {
    return encodeValueToString(this.get(), this.annotations)
}

interface MatchValueEncoder {
    fun acceptValue(value: Any): Boolean
    fun encodeToString(value: Any, annoList: List<Annotation>): String
}

fun interface ValueEncoder {
    fun encodeToString(value: Any, annoList: List<Annotation>): String
}

object ValueEncodeManager {
    fun push(fromClass: KClass<*>, encoder: ValueEncoder) {
        valuetEncoderMap[fromClass] = encoder
    }

    fun pushMatch(matchEncoder: MatchValueEncoder) {
        if (matchValueEncoders.isEmpty()) matchValueEncoders.add(matchEncoder)
        else matchValueEncoders.add(0, matchEncoder)
    }

    fun encodeValue(value: Any?, annoList: List<Annotation> = emptyList()): String? {
        return encodeValueToString(value, annoList)
    }
}

fun encodeValueToString(value: Any?, annoList: List<Annotation>): String? {
    if (value == null) return null
    if (value is String) return value
    val cls = value::class
    val enc = valuetEncoderMap[cls]
    if (enc != null) return enc.encodeToString(value, annoList)
    for (m in matchValueEncoders) {
        if (m.acceptValue(value)) return m.encodeToString(value, annoList)
    }
    return DefaultValueEncoder.encodeToString(value, annoList)
}

private fun findListChar(annoList: List<Annotation>): String {
    return (annoList.firstNotNullOfOrNull { it as? SepChar }?.list ?: ',').toString()
}

private fun findDatePattern(annoList: List<Annotation>, other: String): String {
    val dateFormat: DatePattern? = annoList.firstNotNullOfOrNull { it as? DatePattern }
    return dateFormat?.format ?: other
}

private object ArrayMatchValueEncoder : MatchValueEncoder {
    override fun acceptValue(value: Any): Boolean {
        return value is Array<*>
    }

    override fun encodeToString(value: Any, annoList: List<Annotation>): String {
        val sep = findListChar(annoList)
        val len: Int = java.lang.reflect.Array.getLength(value)
        val ls = ArrayList<String>(len + 1)
        for (i in 0 until len) {
            val v = java.lang.reflect.Array.get(value, i)
            ls += encodeValueToString(v, annoList) ?: continue
        }
        return ls.joinToString(sep)
    }
}

private object MapMatchValueEncoder : MatchValueEncoder {
    override fun acceptValue(value: Any): Boolean {
        return value is Map<*, *>
    }

    override fun encodeToString(value: Any, annoList: List<Annotation>): String {
        val mapChar = annoList.firstNotNullOfOrNull { it as? SepChar }
        val kvSep = (mapChar?.map ?: ':').toString()
        value as Map<*, *>
        val ls = ArrayList<String>(128)
        for (e in value) {
            val a = encodeValueToString(e.key, annoList) ?: continue
            val b = encodeValueToString(e.value, annoList) ?: continue
            ls += "$a$kvSep$b"
        }
        return ls.joinToString((mapChar?.list ?: ',').toString())
    }
}

private object IterableMatchValueEncoder : MatchValueEncoder {
    override fun acceptValue(value: Any): Boolean {
        return value is Iterable<*>
    }

    override fun encodeToString(value: Any, annoList: List<Annotation>): String {
        val sep = findListChar(annoList)
        value as Iterable<*>
        val ls = ArrayList<String>(128)
        for (v in value) {
            ls += encodeValueToString(v, annoList) ?: continue
        }
        return ls.joinToString(sep)
    }
}

private val matchValueEncoders: ArrayList<MatchValueEncoder> = arrayListOf(ArrayMatchValueEncoder, MapMatchValueEncoder, IterableMatchValueEncoder)

private object DefaultValueEncoder : ValueEncoder {
    override fun encodeToString(value: Any, annoList: List<Annotation>): String {
        return value.toString()
    }
}

inline fun <reified T : Annotation> List<Annotation>.firstAnno(): T? {
    return this.firstNotNullOfOrNull { it as? T }
}

object NumberValueEncoder : ValueEncoder {
    override fun encodeToString(value: Any, annoList: List<Annotation>): String {
        annoList.firstAnno<NumberPattern>()?.let { return (value as Number).format(it.pattern) }
        annoList.firstAnno<StringFormat>()?.let { return String.format(it.pattern, value) }
        annoList.firstAnno<Decimal>()?.let { dm ->
            if (dm.pattern.isNotEmpty()) return (value as Number).format(dm.pattern)
            return (value as Number).format(0, dm.scale)
        }
        return value.toString()
    }
}

@Suppress("UNCHECKED_CAST")
private val valuetEncoderMap: HashMap<KClass<*>, ValueEncoder> = hashMapOf(
    Byte::class to NumberValueEncoder,
    Short::class to NumberValueEncoder,
    Int::class to NumberValueEncoder,
    Long::class to NumberValueEncoder,
    Float::class to NumberValueEncoder,
    Double::class to NumberValueEncoder,
    BigDecimal::class to NumberValueEncoder,

    String::class to DefaultValueEncoder,
    Boolean::class to DefaultValueEncoder,
    Char::class to DefaultValueEncoder,

    UUID::class to DefaultValueEncoder,
    URI::class to DefaultValueEncoder,
    URL::class to DefaultValueEncoder,

    KsonObject::class to DefaultValueEncoder,
    KsonArray::class to DefaultValueEncoder,
    KsonBool::class to DefaultValueEncoder,
    KsonString::class to DefaultValueEncoder,
    KsonNum::class to DefaultValueEncoder,
    KsonNull::class to DefaultValueEncoder,

    ByteArray::class to ValueEncoder { value, annoList -> (value as ByteArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    ShortArray::class to ValueEncoder { value, annoList -> (value as ShortArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    IntArray::class to ValueEncoder { value, annoList -> (value as IntArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    LongArray::class to ValueEncoder { value, annoList -> (value as LongArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    FloatArray::class to ValueEncoder { value, annoList -> (value as FloatArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    DoubleArray::class to ValueEncoder { value, annoList -> (value as DoubleArray).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },

    BooleanArray::class to ValueEncoder { value, annoList -> (value as BooleanArray).joinToString(findListChar(annoList)) { it.toString() } },
    CharArray::class to ValueEncoder { value, annoList -> (value as ByteArray).joinToString(findListChar(annoList)) { it.toString() } },

    Array<Byte>::class to ValueEncoder { value, annoList -> (value as Array<Byte>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    Array<Short>::class to ValueEncoder { value, annoList -> (value as Array<Short>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    Array<Int>::class to ValueEncoder { value, annoList -> (value as Array<Int>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    Array<Long>::class to ValueEncoder { value, annoList -> (value as Array<Long>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    Array<Float>::class to ValueEncoder { value, annoList -> (value as Array<Float>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },
    Array<Double>::class to ValueEncoder { value, annoList -> (value as Array<Double>).joinToString(findListChar(annoList)) { NumberValueEncoder.encodeToString(it, annoList) } },

    Array<Boolean>::class to ValueEncoder { value, annoList -> (value as Array<Boolean>).joinToString(findListChar(annoList)) { it.toString() } },
    Array<Char>::class to ValueEncoder { value, annoList -> (value as Array<Char>).joinToString(findListChar(annoList)) { it.toString() } },

    Time::class to ValueEncoder { value, annoList -> (value as Time).format(findDatePattern(annoList, DateTime.FORMAT_TIME)) },
    LocalTime::class to ValueEncoder { value, annoList -> (value as LocalTime).format(findDatePattern(annoList, DateTime.FORMAT_TIME)) },

    Date::class to ValueEncoder { value, annoList -> (value as Date).format(findDatePattern(annoList, DateTime.FORMAT_DATE)) },
    java.sql.Date::class to ValueEncoder { value, annoList -> (value as java.sql.Date).format(findDatePattern(annoList, DateTime.FORMAT_DATE)) },
    LocalDate::class to ValueEncoder { value, annoList -> (value as LocalDate).format(findDatePattern(annoList, DateTime.FORMAT_DATE)) },

    Timestamp::class to ValueEncoder { value, annoList -> (value as Timestamp).format(findDatePattern(annoList, DateTime.FORMAT_DATE_TIME)) },
    LocalDateTime::class to ValueEncoder { value, annoList -> (value as LocalDateTime).format(findDatePattern(annoList, DateTime.FORMAT_DATE_TIME)) },
    DateTime::class to ValueEncoder { value, annoList -> (value as DateTime).format(findDatePattern(annoList, DateTime.FORMAT_DATE_TIME)) },

    )