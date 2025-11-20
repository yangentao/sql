package entao

import io.github.yangentao.anno.DatePattern
import io.github.yangentao.anno.ModelField
import io.github.yangentao.kson.KsonNum
import io.github.yangentao.sql.TableMigrater
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import io.github.yangentao.sql.toJson
import io.github.yangentao.types.DateTime
import io.github.yangentao.types.TargetInfo
import io.github.yangentao.types.ValueDecoder
import io.github.yangentao.types.returnClass
import io.github.yangentao.xlog.logd
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.test.Test

class ADate : TableModel() {
    @ModelField(primaryKey = true, autoInc = 1)
    var id: Int by model

    @ModelField
    var localDateTime: LocalDateTime? by model

    @ModelField
    var localDate: LocalDate? by model

    @ModelField
    var localTime: LocalTime? by model

    @ModelField
    var sqlDate: java.sql.Date? by model

    @ModelField
    var sqlTime: java.sql.Time? by model

    @ModelField
    var stamp: java.sql.Timestamp? by model

    override fun toString(): String {
        return this.toJson().toString()
    }

    companion object : TableModelClass<ADate>()
}

class DateTest {

    @Test
    fun date() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val c = HarePool.pick()
        TableMigrater(c, ADate::class)

        val now = DateTime()
        val a = ADate()
        a.sqlTime = now.time
        a.sqlDate = now.dateSQL
        a.stamp = now.timestamp
        a.localDate = now.localDate
        a.localTime = now.localTime
        a.localDateTime = now.localDateTime
        a.insert()
        ADate.dumpTable()
        // id=1, ldt=2025-05-19T16:30:20.529, ld=2025-05-19, lt=16:30:20.529, dt=1747643420529, tm=1747643420529, stamp=1747643420529,

        val b = ADate.oneByKey(1) ?: return
        for (e in b.model.entries) {
            logd(e.key, e.value, e.value!!::class.simpleName)
        }

        logd(b.localTime)
        println(b.toString())

    }

}

fun main() {

    val p = ADate::localTime
    val src: Int = DateTime.now.timeInMillis.toInt()
    val target = TargetInfo(p.returnClass)
    val d = DateDecoder
    if (d.accept(p.returnClass, src::class)) {
        println("Accept")
        val v = d.decode(target, src)
        println("value: $v  ")
    } else {
        println("Not accept")
    }
}

private object DateDecoder : ValueDecoder() {
    private val clsSet: Set<KClass<*>> = setOf(
        java.util.Date::class, java.sql.Date::class, java.sql.Time::class, Timestamp::class,
        LocalDate::class, LocalTime::class, LocalDateTime::class
    )

    override fun accept(target: KClass<*>, source: KClass<*>): Boolean {
        return target in clsSet && (source in clsSet || source == Long::class || source == String::class)
    }

    private fun toDateTime(info: TargetInfo, value: Any): DateTime? {
        when (value) {
            is java.sql.Date -> return DateTime.from(value)
            is java.sql.Time -> return DateTime.from(value)
            is java.sql.Timestamp -> return DateTime.from(value)
            is java.util.Date -> return DateTime.from(value)
            is LocalDate -> return DateTime.from(value)
            is LocalTime -> return DateTime.from(value)
            is LocalDateTime -> return DateTime.from(value)
            is Long -> return DateTime(value)
            is String -> {
                if (value.isEmpty()) return null
                val dp: DatePattern? = info.findAnnotation()
                return if (dp != null) {
                    DateTime.parse(dp.format, value) ?: error("Parse error, ${info.clazz},  $value")
                } else {
                    DateTime.parseDate(value) ?: DateTime.parseDateTime(value) ?: DateTime.parseTime(value) ?: error("Parse error, ${info.clazz},  value='$value'")
                }
            }

            is KsonNum -> return DateTime(value.data.toLong())

            else -> error("Unsupport type, ${info.clazz},  $value")
        }
    }

    override fun decode(targetInfo: TargetInfo, value: Any): Any? {
        val dt = toDateTime(targetInfo, value) ?: return null
        return when (targetInfo.clazz) {
            java.util.Date::class -> dt.date
            java.sql.Date::class -> dt.dateSQL
            java.sql.Time::class -> dt.time
            java.sql.Timestamp::class -> dt.timestamp
            LocalDate::class -> dt.localDate
            LocalTime::class -> dt.localTime
            LocalDateTime::class -> dt.localDateTime
            else -> error("NOT support type: ${targetInfo.clazz}")
        }
    }
}