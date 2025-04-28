package io.github.yangentao.reflect

//import dev.entao.web.sql.*

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.anno.NullValue
import io.github.yangentao.anno.SepChar
import io.github.yangentao.kson.KsonArray
import io.github.yangentao.kson.KsonNum
import io.github.yangentao.kson.KsonObject
import io.github.yangentao.sql.ModelField
import io.github.yangentao.sql.list
import io.github.yangentao.sql.listBool
import io.github.yangentao.sql.listByte
import io.github.yangentao.sql.listChar
import io.github.yangentao.sql.listDouble
import io.github.yangentao.sql.listFloat
import io.github.yangentao.sql.listInt
import io.github.yangentao.sql.listLong
import io.github.yangentao.sql.listShort
import io.github.yangentao.sql.listString
import java.net.URI
import java.net.URL
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.iterator
import kotlin.reflect.*
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf

fun KType.decodeValue(value: Any?): Any? {
    return decodeValueToType(value, this.classifier as KClass<*>, this.annotations, this.genericArgs)
}

fun KParameter.decodeValue(value: Any?): Any? {
    return decodeValueToType(value, this.type.classifier as KClass<*>, this.annotations, this.type.genericArgs)
}

@Suppress("UNCHECKED_CAST")
fun <R> KProperty<R>.decodeValueTyped(value: Any?): R? {
    return this.decodeValue(value) as? R
}

fun KProperty<*>.decodeValue(value: Any?): Any? {
    return decodeValueToType(value, this.returnType.classifier as KClass<*>, this.annotations, this.returnType.genericArgs)
}

fun KClass<*>.decodeValue(value: Any?): Any? {
    return decodeValueToType(value, this, this.annotations, emptyList())
}

inline fun <reified T : Any> KClass<T>.decode(value: Any?): T? {
    return this.decodeValue(value) as? T
}

object ValueDecodeManager {
    fun push(d: TypeDecoder) {
        decodeTypeMap[d.toClass] = d
    }

    fun pushMatch(d: MatchTypeDecoder) {
        decodeMatchList.add(0, d)
    }

    fun decodeValueTo(value: Any?, toClass: KClass<*>, annoList: List<Annotation> = emptyList(), genericArgs: List<KTypeProjection> = emptyList()): Any? {
        return decodeValueToType(value, toClass, annoList, genericArgs)
    }
}

fun decodeValueToType(value: Any?, toClass: KClass<*>, annoList: List<Annotation> = emptyList(), genericArgs: List<KTypeProjection> = emptyList()): Any? {
    if (value == null) {
        val nullStr: String? = findNullValue(toClass, annoList)?.value
        if (nullStr != null) {
            return decodeValueToType(nullStr, toClass, annoList, genericArgs)
        }
        val f: String? = annoList.firstTyped<ModelField>()?.defaultValue
        if (f != null) {
            return decodeValueToType(f, toClass, annoList, genericArgs)
        }
        return null

    }
    if (value::class == toClass) return value
    val d = decodeTypeMap[toClass]
    if (d != null) return d.decode(value, annoList, genericArgs)
    for (e in decodeMatchList) {
        if (e.isMatch(toClass)) return e.decode(value, toClass, annoList, genericArgs)
    }
    error("NOT support decode: $value  to class $toClass")
}

private val decodeMatchList: ArrayList<MatchTypeDecoder> = arrayListOf(ArrayTypeDecoder, ListTypeDecoder, SetTypeDecoder, MapTypeDecoder)

private val listDecoders: List<TypeDecoder> = listOf(
    StringTypeDecoder,
    CharTypeDecoder,
    BoolTypeDecoder,
    ByteTypeDecoder,
    ShortTypeDecoder,
    IntTypeDecoder,
    LongTypeDecoder,
    FloatTypeDecoder,
    DoubleTypeDecoder,
    YsonObjectTypeDecoder,
    YsonArrayTypeDecoder,
    BooleanArrayTypeDecoder,
    CharArrayTypeDecoder,
    ByteArrayTypeDecoder,
    ShortArrayTypeDecoder,
    IntArrayTypeDecoder,
    LongArrayTypeDecoder,
    FloatArrayTypeDecoder,
    DoubleArrayTypeDecoder,
    ArrayBooleanTypeDecoder,
    ArrayByteTypeDecoder,
    ArrayShortTypeDecoder,
    ArrayIntTypeDecoder,
    ArrayLongTypeDecoder,
    ArrayFloatTypeDecoder,
    ArrayDoubleTypeDecoder,
    ArrayStringTypeDecoder,
    UUIDTypeDecoder,
    URLTypeDecoder,
    URITypeDecoder,
    UtilDateTypeDecoder,
    SQLDateTypeDecoder,
    TimeTypeDecoder,
    TimestampTypeDecoder,
    LocalDateTypeDecoder,
    LocalTimeTypeDecoder,
    LocalDateTimeTypeDecoder
)
private val decodeTypeMap: HashMap<KClass<*>, TypeDecoder> = HashMap<KClass<*>, TypeDecoder>().apply {
    for (d in listDecoders) {
        put(d.toClass, d)
    }
}

private fun splitByAnno(value: String, annoList: List<Annotation>): List<String> {
    val sc = findSplitChar(annoList)
    return value.split(sc?.list ?: ',').map { it.trim() }
}

abstract class MatchTypeDecoder {
    abstract fun isMatch(toClass: KClass<*>): Boolean
    open fun decode(value: Any?, toClasss: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        if (value == null) return onNull(toClasss, annoList, genericArgs)
        if (value::class == toClasss) return onSameType(value, toClasss, annoList, genericArgs)
        return decodeValue(value, toClasss, annoList, genericArgs)
    }

    open fun onNull(toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        val an: NullValue? = findNullValue(toClass, annoList)
        if (an != null) {
            this.decodeValue(an.value, toClass, annoList, genericArgs)
        }
        return null
    }

    open fun onSameType(value: Any?, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        return value
    }

    abstract fun decodeValue(value: Any, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any?
    protected fun prepareItems(value: Any, annoList: List<Annotation>): Iterable<Any?> {
        return when (value) {
            is Set<*> -> value
            is List<*> -> value
            is Array<*> -> value.toList()
            is java.sql.Array -> value.resultSet.list { objectValue(2) }
            is String -> splitByAnno(value, annoList)

            is ByteArray -> value.toList()
            is ShortArray -> value.toList()
            is IntArray -> value.toList()
            is LongArray -> value.toList()
            is FloatArray -> value.toList()
            is DoubleArray -> value.toList()
            is BooleanArray -> value.toList()
            is CharArray -> value.toList()

            else -> error("Type  dismatch $value ")
        }
    }

    protected fun failed(value: Any): Nothing {
        error("Type  dismatch $value ")
    }
}

abstract class TypeDecoder(val toClass: KClass<*>) {
    open fun decode(value: Any?, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        if (value == null) return onNull(annoList, genericArgs)
        if (value::class == toClass) return onSameType(value, annoList, genericArgs)
        return decodeValue(value, annoList, genericArgs)
    }

    open fun onNull(annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        val an: NullValue? = findNullValue(toClass, annoList)
        if (an != null) {
            this.decodeValue(an.value, annoList, genericArgs)
        }
        return null
    }

    open fun onSameType(value: Any?, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        return value
    }

    abstract fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any?
    protected fun failed(value: Any): Nothing {
        error("Type  dismatch, type: ${value::class}, value: $value ")
    }
}

private object StringTypeDecoder : TypeDecoder(String::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): String? {
        return ValueEncodeManager.encodeValue(value, annoList)
    }
}

private object CharTypeDecoder : TypeDecoder(Char::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Char? {
        when (value) {
            is String -> if (value.length == 1) return value.first()
        }
        return null
    }
}

private object BoolTypeDecoder : TypeDecoder(Boolean::class) {
    private val trueSet: Set<Any> = setOf(true, 1, "1", "true", "on", "yes")
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Boolean {
        return trueSet.contains(value)
    }
}

private object IntTypeDecoder : TypeDecoder(Int::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Int? {
        return when (value) {
            is Number -> value.toInt()
            is String -> value.toIntOrNull()
            is KsonNum -> value.data.toInt()
            else -> failed(value)
        }
    }
}

private object LongTypeDecoder : TypeDecoder(Long::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Long? {
        return when (value) {
            is Number -> value.toLong()
            is String -> value.toLongOrNull()
            is KsonNum -> value.data.toLong()
            else -> failed(value)
        }
    }
}

private object ByteTypeDecoder : TypeDecoder(Byte::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Byte? {
        return when (value) {
            is Number -> value.toByte()
            is String -> value.toByteOrNull()
            is KsonNum -> value.data.toByte()
            else -> failed(value)
        }
    }
}

private object ShortTypeDecoder : TypeDecoder(Short::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Short? {
        return when (value) {
            is Number -> value.toShort()
            is String -> value.toShortOrNull()
            is KsonNum -> value.data.toShort()
            else -> failed(value)
        }
    }
}

private object FloatTypeDecoder : TypeDecoder(Float::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Float? {
        return when (value) {
            is Number -> value.toFloat()
            is String -> value.toFloatOrNull()
            is KsonNum -> value.data.toFloat()
            else -> failed(value)
        }
    }
}

private object DoubleTypeDecoder : TypeDecoder(Double::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Double? {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull()
            is KsonNum -> value.data.toDouble()
            else -> failed(value)
        }
    }
}

private object YsonObjectTypeDecoder : TypeDecoder(KsonObject::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): KsonObject {
        return when (value) {
            is String -> KsonObject(value)
            else -> failed(value)
        }
    }
}

private object YsonArrayTypeDecoder : TypeDecoder(KsonArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): KsonArray {
        return when (value) {
            is String -> KsonArray(value)
            else -> failed(value)
        }
    }
}

private object CharArrayTypeDecoder : TypeDecoder(CharArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): CharArray {
        return when (value) {
            is java.sql.Array -> value.listChar.toCharArray()
            is String -> value.toCharArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Char::class, annoList) as? Char }.toCharArray()
            else -> failed(value)
        }
    }
}

private object BooleanArrayTypeDecoder : TypeDecoder(BooleanArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): BooleanArray {
        return when (value) {
            is java.sql.Array -> value.listBool.toBooleanArray()
            is String -> splitByAnno(value, annoList).mapNotNull { decodeValueToType(value, Boolean::class, annoList) as? Boolean }.toBooleanArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Boolean::class, annoList) as? Boolean }.toBooleanArray()
            is LongArray -> value.map { it != 0L }.toBooleanArray()
            is IntArray -> value.map { it != 0 }.toBooleanArray()
            is ShortArray -> value.map { it.toInt() != 0 }.toBooleanArray()
            is ByteArray -> value.map { it.toInt() != 0 }.toBooleanArray()
            is BooleanArray -> value
            else -> failed(value)
        }
    }
}

private object ByteArrayTypeDecoder : TypeDecoder(ByteArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): ByteArray {
        return when (value) {
            is java.sql.Array -> value.listByte.toByteArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toByteOrNull() }.toByteArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Byte::class, annoList) as? Byte }.toByteArray()
            is LongArray -> value.map { it.toByte() }.toByteArray()
            is IntArray -> value.map { it.toByte() }.toByteArray()
            is ShortArray -> value.map { it.toByte() }.toByteArray()
            is ByteArray -> value
            is BooleanArray -> value.map { if (it) 1.toByte() else 0.toByte() }.toByteArray()
            else -> failed(value)
        }
    }
}

private object ShortArrayTypeDecoder : TypeDecoder(ShortArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): ShortArray {
        return when (value) {
            is java.sql.Array -> value.listShort.toShortArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toShortOrNull() }.toShortArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Short::class, annoList) as? Short }.toShortArray()
            is LongArray -> value.map { it.toShort() }.toShortArray()
            is IntArray -> value.map { it.toShort() }.toShortArray()
            is ShortArray -> value
            is ByteArray -> value.map { it.toShort() }.toShortArray()
            is BooleanArray -> value.map { if (it) 1.toShort() else 0.toShort() }.toShortArray()
            else -> failed(value)
        }
    }
}

private object IntArrayTypeDecoder : TypeDecoder(IntArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): IntArray {
        return when (value) {
            is java.sql.Array -> value.listInt.toIntArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toIntOrNull() }.toIntArray()
            is LongArray -> value.map { it.toInt() }.toIntArray()
            is IntArray -> value
            is ShortArray -> value.map { it.toInt() }.toIntArray()
            is ByteArray -> value.map { it.toInt() }.toIntArray()
            is BooleanArray -> value.map { if (it) 1 else 0 }.toIntArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Int::class, annoList, emptyList()) as? Int }.toIntArray()
            else -> failed(value)
        }
    }
}

private object LongArrayTypeDecoder : TypeDecoder(LongArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): LongArray {
        return when (value) {
            is java.sql.Array -> value.listLong.toLongArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toLongOrNull() }.toLongArray()
            is IntArray -> value.map { it.toLong() }.toLongArray()
            is ShortArray -> value.map { it.toLong() }.toLongArray()
            is ByteArray -> value.map { it.toLong() }.toLongArray()
            is BooleanArray -> value.map { if (it) 1L else 0L }.toLongArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Long::class, annoList) as? Long }.toLongArray()
            else -> failed(value)
        }
    }
}

private object FloatArrayTypeDecoder : TypeDecoder(FloatArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): FloatArray {
        return when (value) {
            is java.sql.Array -> value.listFloat.toFloatArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toFloatOrNull() }.toFloatArray()
            is DoubleArray -> value.map { it.toFloat() }.toFloatArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Float::class, annoList) as? Float }.toFloatArray()
            else -> failed(value)
        }
    }
}

private object DoubleArrayTypeDecoder : TypeDecoder(DoubleArray::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): DoubleArray {
        return when (value) {
            is java.sql.Array -> value.listDouble.toDoubleArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toDoubleOrNull() }.toDoubleArray()
            is FloatArray -> value.map { it.toDouble() }.toDoubleArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Double::class, annoList) as? Double }.toDoubleArray()
            else -> failed(value)
        }
    }
}

private object ArrayBooleanTypeDecoder : TypeDecoder(Array<Boolean>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Boolean> {
        return when (value) {
            is java.sql.Array -> value.listBool.toTypedArray()
            is String -> splitByAnno(value, annoList).map { it.toBoolean() }.toTypedArray()
            is LongArray -> value.map { it != 0L }.toTypedArray()
            is IntArray -> value.map { it != 0 }.toTypedArray()
            is ShortArray -> value.map { it.toInt() != 0 }.toTypedArray()
            is ByteArray -> value.map { it.toInt() != 0 }.toTypedArray()
            is BooleanArray -> value.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Boolean::class, annoList) as? Boolean }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayByteTypeDecoder : TypeDecoder(Array<Byte>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Byte> {
        return when (value) {
            is java.sql.Array -> value.listByte.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toByteOrNull() }.toTypedArray()
            is LongArray -> value.map { it.toByte() }.toTypedArray()
            is IntArray -> value.map { it.toByte() }.toTypedArray()
            is ShortArray -> value.map { it.toByte() }.toTypedArray()
            is ByteArray -> value.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Byte::class, annoList) as? Byte }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayShortTypeDecoder : TypeDecoder(Array<Short>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Short> {
        return when (value) {
            is java.sql.Array -> value.listShort.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toShortOrNull() }.toTypedArray()
            is LongArray -> value.map { it.toShort() }.toTypedArray()
            is IntArray -> value.map { it.toShort() }.toTypedArray()
            is ShortArray -> value.toTypedArray()
            is ByteArray -> value.map { it.toShort() }.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Short::class, annoList) as? Short }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayIntTypeDecoder : TypeDecoder(Array<Int>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Int> {
        return when (value) {
            is java.sql.Array -> value.listInt.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toIntOrNull() }.toTypedArray()
            is LongArray -> value.map { it.toInt() }.toTypedArray()
            is IntArray -> value.toTypedArray()
            is ShortArray -> value.map { it.toInt() }.toTypedArray()
            is ByteArray -> value.map { it.toInt() }.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Int::class, emptyList(), emptyList()) as? Int }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayLongTypeDecoder : TypeDecoder(Array<Long>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Long> {
        return when (value) {
            is java.sql.Array -> value.listLong.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toLongOrNull() }.toTypedArray()
            is LongArray -> value.toTypedArray()
            is IntArray -> value.map { it.toLong() }.toTypedArray()
            is ShortArray -> value.map { it.toLong() }.toTypedArray()
            is ByteArray -> value.map { it.toLong() }.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Long::class, annoList) as? Long }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayFloatTypeDecoder : TypeDecoder(Array<Float>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Float> {
        return when (value) {
            is java.sql.Array -> value.listFloat.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toFloatOrNull() }.toTypedArray()
            is FloatArray -> value.toTypedArray()
            is DoubleArray -> value.map { it.toFloat() }.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Float::class, annoList) as? Float }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayDoubleTypeDecoder : TypeDecoder(Array<Double>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<Double> {
        return when (value) {
            is java.sql.Array -> value.listDouble.toTypedArray()
            is String -> splitByAnno(value, annoList).mapNotNull { it.toDoubleOrNull() }.toTypedArray()
            is FloatArray -> value.map { it.toDouble() }.toTypedArray()
            is DoubleArray -> value.toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, Double::class, annoList) as? Double }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object ArrayStringTypeDecoder : TypeDecoder(Array<String>::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Array<String> {
        return when (value) {
            is java.sql.Array -> value.listString.toTypedArray()
            is String -> splitByAnno(value, annoList).toTypedArray()
            is Array<*> -> value.mapNotNull { decodeValueToType(it, String::class, annoList) as? String }.toTypedArray()
            else -> failed(value)
        }
    }
}

private object UUIDTypeDecoder : TypeDecoder(UUID::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): UUID {
        return UUID.fromString(value.toString())
    }
}

@Suppress("DEPRECATION")
private object URLTypeDecoder : TypeDecoder(URL::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): URL {
        return URL(value.toString())
    }
}

private object URITypeDecoder : TypeDecoder(URI::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): URI {
        return URI(value.toString())
    }
}

private object UtilDateTypeDecoder : TypeDecoder(Date::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Date? {
        return when (value) {
            is Number -> Date(value.toLong())
            is Time -> DateTime.from(value).date
            is Timestamp -> DateTime.from(value).date
            is java.sql.Date -> DateTime.from(value).date
            is LocalDate -> DateTime.from(value).date
            is LocalDateTime -> DateTime.from(value).date
            is DateTime -> value.date
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_DATE
                DateTime.parse(fmt, value)?.date
            }

            else -> failed(value)
        }
    }
}

private object SQLDateTypeDecoder : TypeDecoder(java.sql.Date::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): java.sql.Date? {
        return when (value) {
            is Number -> java.sql.Date(value.toLong())
            is Time -> DateTime.from(value).dateSQL
            is Timestamp -> DateTime.from(value).dateSQL
            is Date -> DateTime.from(value).dateSQL
            is LocalDateTime -> DateTime.from(value).dateSQL
            is LocalDate -> DateTime.from(value).dateSQL
            is DateTime -> value.dateSQL
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_DATE
                DateTime.parse(fmt, value)?.dateSQL
            }

            else -> failed(value)
        }
    }
}

private object TimeTypeDecoder : TypeDecoder(Time::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Time? {
        return when (value) {
            is Number -> Time(value.toLong())
            is Timestamp -> Time(value.time)
            is java.sql.Date -> Time(value.time)
            is Date -> Time(value.time)
            is LocalDateTime -> DateTime.from(value).time
            is LocalTime -> DateTime.from(value).time
            is DateTime -> value.time
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_TIME
                DateTime.parse(fmt, value)?.time
            }

            else -> failed(value)
        }
    }
}

private object TimestampTypeDecoder : TypeDecoder(Timestamp::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Timestamp? {
        return when (value) {
            is Number -> Timestamp(value.toLong())
            is Time -> Timestamp(value.time)
            is java.sql.Date -> Timestamp(value.time)
            is Date -> Timestamp(value.time)
            is LocalDateTime -> DateTime.from(value).timestamp
            is LocalDate -> DateTime.from(value).timestamp
            is DateTime -> value.timestamp
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_DATE_TIME
                DateTime.parse(fmt, value)?.timestamp
            }

            else -> failed(value)
        }
    }
}

private object LocalTimeTypeDecoder : TypeDecoder(LocalTime::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): LocalTime? {
        return when (value) {
            is Number -> LocalDateTime.ofInstant(Date(value.toLong()).toInstant(), ZoneId.systemDefault()).toLocalTime()
            is Time -> DateTime.from(value).localTime
            is Timestamp -> DateTime.from(value).localTime
            is java.sql.Date -> DateTime.from(value).localTime
            is Date -> DateTime.from(value).localTime
            is LocalDateTime -> value.toLocalTime()
            is DateTime -> value.localTime
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_TIME
                LocalTime.parse(value, DateTimeFormatter.ofPattern(fmt))
            }

            else -> failed(value)
        }
    }
}

private object LocalDateTypeDecoder : TypeDecoder(LocalDate::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): LocalDate? {
        return when (value) {
            is Number -> LocalDateTime.ofInstant(Date(value.toLong()).toInstant(), ZoneId.systemDefault()).toLocalDate()
            is Timestamp -> DateTime.from(value).localDate
            is java.sql.Date -> DateTime.from(value).localDate
            is Date -> DateTime.from(value).localDate
            is LocalDateTime -> value.toLocalDate()
            is DateTime -> value.localDate
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_DATE
                LocalDate.parse(value, DateTimeFormatter.ofPattern(fmt))
            }

            else -> failed(value)
        }
    }
}

private object LocalDateTimeTypeDecoder : TypeDecoder(LocalDateTime::class) {
    override fun decodeValue(value: Any, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): LocalDateTime? {
        return when (value) {
            is Number -> LocalDateTime.ofInstant(Date(value.toLong()).toInstant(), ZoneId.systemDefault())
            is Time -> DateTime.from(value).localDateTime
            is Timestamp -> DateTime.from(value).localDateTime
            is java.sql.Date -> DateTime.from(value).localDateTime
            is Date -> DateTime.from(value).localDateTime
            is LocalDate -> value.atTime(0, 0)
            is LocalTime -> value.atDate(LocalDate.of(1970, 1, 1))
            is DateTime -> value.localDateTime
            is String -> {
                if (value.isEmpty()) return null
                val fmt = annoList.firstNotNullOfOrNull { it as? DatePattern }?.format ?: DateTime.FORMAT_DATE_TIME
                LocalDateTime.parse(value, DateTimeFormatter.ofPattern(fmt))
            }

            else -> failed(value)
        }
    }
}

private object ArrayTypeDecoder : MatchTypeDecoder() {

    override fun isMatch(toClass: KClass<*>): Boolean {
        return toClass.java.isArray
    }

    override fun decodeValue(value: Any, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        val eleCls = toClass.java.componentType
        val ls: List<Any?> = prepareItems(value, annoList).toList()
        val ar = java.lang.reflect.Array.newInstance(eleCls, ls.size)
        for (i in ls.indices) {
            java.lang.reflect.Array.set(ar, i, decodeValueToType(ls[i], eleCls.kotlin, emptyList(), emptyList()))
        }
        return ar
    }
}

private object ListTypeDecoder : MatchTypeDecoder() {

    override fun isMatch(toClass: KClass<*>): Boolean {
        return toClass.isSubclassOf(List::class)
    }

    override fun decodeValue(value: Any, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        val eleType: KClass<*> = genericArgs.firstOrNull()?.type?.classifier as? KClass<*> ?: return null
        val ls: Iterable<Any?> = prepareItems(value, annoList)
        val valueList: List<Any?> = ls.map { decodeValueToType(it, eleType, emptyList(), emptyList()) }
        if (!toClass.isAbstract && toClass.isSubclassOf(MutableList::class)) {
            @Suppress("UNCHECKED_CAST")
            val muList: MutableList<Any?> = toClass.createInstance() as MutableList<Any?>
            muList.addAll(valueList)
            return muList
        }
        return ArrayList(valueList)
    }
}

private object SetTypeDecoder : MatchTypeDecoder() {
    override fun isMatch(toClass: KClass<*>): Boolean {
        return toClass.isSubclassOf(Set::class)
    }

    override fun decodeValue(value: Any, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        val eleType: KClass<*> = genericArgs.firstOrNull()?.type?.classifier as? KClass<*> ?: return null
        val ls: Iterable<Any?> = prepareItems(value, annoList)
        val valueSet: LinkedHashSet<Any?> = LinkedHashSet(ls.map { decodeValueToType(it, eleType, emptyList(), emptyList()) })
        if (!toClass.isAbstract && toClass.isSubclassOf(MutableSet::class)) {
            @Suppress("UNCHECKED_CAST")
            val muList: MutableSet<Any?> = toClass.createInstance() as MutableSet<Any?>
            muList.addAll(valueSet)
            return muList
        }
        return valueSet
    }

}

private object MapTypeDecoder : MatchTypeDecoder() {
    override fun isMatch(toClass: KClass<*>): Boolean {
        return toClass.isSubclassOf(Map::class)
    }

    override fun decodeValue(value: Any, toClass: KClass<*>, annoList: List<Annotation>, genericArgs: List<KTypeProjection>): Any? {
        if (genericArgs.size < 2) return null
        val keyType: KClass<*> = genericArgs[0].type?.classifier as? KClass<*> ?: return null
        val valType: KClass<*> = genericArgs[1].type?.classifier as? KClass<*> ?: return null
        val valueMap: LinkedHashMap<Any, Any?> = LinkedHashMap<Any, Any?>()
        if (value is Map<*, *>) {
            for ((k, v) in value) {
                valueMap[k as Any] = decodeValueToType(v, valType, emptyList(), emptyList())
            }
        } else if (value is String) {
            val mapChars = findSplitChar(annoList)
            val pls = value.split(mapChars?.list ?: ',').map { it.split(mapChars?.map ?: ':') }
            for (p in pls) {
                if (p.size == 2) {
                    val k = decodeValueToType(p.first().trim(), keyType, emptyList(), emptyList()) ?: continue
                    val v = decodeValueToType(p[1].trim(), valType, emptyList(), emptyList()) ?: continue
                    valueMap[k] = v
                }
            }
        } else {
            error("Type  dismatch $value ")
        }
        if (!toClass.isAbstract && toClass.isSubclassOf(MutableMap::class)) {
            @Suppress("UNCHECKED_CAST")
            val muMap: MutableMap<Any, Any?> = toClass.createInstance() as MutableMap<Any, Any?>
            muMap.putAll(valueMap)
            return muMap
        }
        return valueMap
    }

}

private fun findNullValue(toClass: KClass<*>, annoList: List<Annotation>): NullValue? {
    return annoList.firstNotNullOfOrNull { it as? NullValue } ?: toClass.findAnnotation()
}

private fun findSplitChar(annoList: List<Annotation>): SepChar? {
    return annoList.firstOrNull { it is SepChar } as? SepChar
}

