@file:Suppress("unused")

package io.github.yangentao.reflect

import java.sql.Time
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * Created by entaoyang@163.com on 2016/12/20.
 */

val Int.SEC_MILLS: Long get() = this * 1000L
val Int.MIN_MILLS: Long get() = this * 60_000L
val Int.HOR_MILLS: Long get() = this * 3600_000L
val Int.DAY_MILLS: Long get() = this * 24 * 3600_000L
val Long.SEC_MILLS: Long get() = this * 1000L
val Long.MIN_MILLS: Long get() = this * 60_000L
val Long.HOR_MILLS: Long get() = this * 3600_000L
val Long.DAY_MILLS: Long get() = this * 24 * 3600_000L

val Long.asDateTime: DateTime get() = DateTime(this)
val Long.asTimestamp: Timestamp get() = Timestamp(this)
val Long.asDateSQL: java.sql.Date get() = java.sql.Date(this)
val Long.asTimeSQL: Time get() = Time(this)

object Now {
    val dateUtil: Date get() = Date(System.currentTimeMillis())
    val dateSQL: java.sql.Date get() = java.sql.Date(System.currentTimeMillis())
    val time: Time get() = Time(System.currentTimeMillis())
    val timestamp: Timestamp get() = Timestamp(System.currentTimeMillis())
    val millis: Long get() = System.currentTimeMillis()
}

fun Date.format(fmt: String): String {
    return SimpleDateFormat(fmt).format(this)
}

fun LocalDate.format(fmt: String): String {
    return this.format(DateTimeFormatter.ofPattern(fmt))
}

fun LocalTime.format(fmt: String): String {
    return this.format(DateTimeFormatter.ofPattern(fmt))
}

fun LocalDateTime.format(fmt: String): String {
    return this.format(DateTimeFormatter.ofPattern(fmt))
}

class DateTime(tm: Long = System.currentTimeMillis(), timeZone: TimeZone = TimeZone.getDefault()) {

    val calendar: Calendar = Calendar.getInstance(timeZone)

    init {
        calendar.timeInMillis = tm
    }

    val date: Date get() = Date(longValue)
    val dateSQL: java.sql.Date get() = java.sql.Date(longValue)
    val time: Time get() = Time(longValue)
    val timestamp: Timestamp get() = Timestamp(longValue)
    val localDate: LocalDate get() = LocalDate.of(this.year, this.monthX, this.day)
    val localTime: LocalTime get() = LocalTime.of(this.hour, this.minute, this.second, this.millSecond * 1_000_000)
    val localDateTime: LocalDateTime get() = LocalDateTime.of(this.year, this.monthX, this.day, this.hour, this.minute, this.second, this.millSecond * 1_000_000)

    var timeInMillis: Long
        get() {
            return calendar.timeInMillis
        }
        set(value) {
            calendar.timeInMillis = value
        }
    var longValue: Long
        get() {
            return calendar.timeInMillis
        }
        set(value) {
            calendar.timeInMillis = value
        }

    //2016
    var year: Int
        get() {
            return calendar.get(Calendar.YEAR)
        }
        set(value) {
            calendar.set(Calendar.YEAR, value)
        }

    //[0-11], 8
    var month: Int
        get() {
            return calendar.get(Calendar.MONTH)
        }
        set(value) {
            calendar.set(Calendar.MONTH, value)
        }

    //[1-12], 8
    var monthX: Int
        get() {
            return calendar.get(Calendar.MONTH) + 1
        }
        set(value) {
            calendar.set(Calendar.MONTH, value - 1)
        }

    //[1-31],  26
    var day: Int
        get() {
            return calendar.get(Calendar.DAY_OF_MONTH)
        }
        set(value) {
            calendar.set(Calendar.DAY_OF_MONTH, value)
        }

    var dayOfYear: Int
        get() {
            return calendar.get(Calendar.DAY_OF_YEAR)
        }
        set(value) {
            calendar.set(Calendar.DAY_OF_YEAR, value)
        }

    //[0-23]
    var hour: Int
        get() {
            return calendar.get(Calendar.HOUR_OF_DAY)
        }
        set(value) {
            calendar.set(Calendar.HOUR_OF_DAY, value)
        }

    //[0-59]
    var minute: Int
        get() {
            return calendar.get(Calendar.MINUTE)
        }
        set(value) {
            calendar.set(Calendar.MINUTE, value)
        }

    //[0-59]
    var second: Int
        get() {
            return calendar.get(Calendar.SECOND)
        }
        set(value) {
            calendar.set(Calendar.SECOND, value)
        }

    //[0-999]
    var millSecond: Int
        get() {
            return calendar.get(Calendar.MILLISECOND)
        }
        set(value) {
            calendar.set(Calendar.MILLISECOND, value)
        }

    var week: Int
        get() {
            return calendar.get(Calendar.DAY_OF_WEEK)
        }
        set(value) {
            calendar.set(Calendar.DAY_OF_WEEK, value)
        }

    val isSunday: Boolean get() = week == Calendar.SUNDAY
    val isMonday: Boolean get() = week == Calendar.MONDAY
    val isTuesday: Boolean get() = week == Calendar.TUESDAY
    val isWednesday: Boolean get() = week == Calendar.WEDNESDAY
    val isThursday: Boolean get() = week == Calendar.THURSDAY
    val isFriday: Boolean get() = week == Calendar.FRIDAY
    val isSaturday: Boolean get() = week == Calendar.SATURDAY

    fun addYear(n: Int): DateTime {
        calendar.add(Calendar.YEAR, n)
        return this
    }

    fun addMonth(n: Int): DateTime {
        calendar.add(Calendar.MONTH, n)
        return this
    }

    fun addDay(n: Int): DateTime {
        calendar.add(Calendar.DAY_OF_MONTH, n)
        return this
    }

    fun addHour(n: Int): DateTime {
        calendar.add(Calendar.HOUR_OF_DAY, n)
        return this
    }

    fun addMinute(n: Int): DateTime {
        calendar.add(Calendar.MINUTE, n)
        return this
    }

    fun addSecond(n: Int): DateTime {
        calendar.add(Calendar.SECOND, n)
        return this
    }

    fun addMillSecond(n: Int): DateTime {
        calendar.add(Calendar.MILLISECOND, n)
        return this
    }

    //yyyy-MM-dd HH:mm:ss
    fun formatDateTime(): String {
        return format(FORMAT_DATE_TIME)
    }

    //yyyy-MM-dd HH:mm:ss.SSS
    fun formatDateTimeX(): String {
        return format(FORMAT_DATE_TIME_X)
    }

    //yyyy-MM-dd
    fun formatDate(): String {
        return format(FORMAT_DATE)
    }

    //HH:mm:ss
    fun formatTime(): String {
        return format(FORMAT_TIME)
    }

    //HH:mm:ss.SSS
    fun formatTimeX(): String {
        return format(FORMAT_TIME_X)
    }

    //HH:mm:ss.SSS
    fun formatTimestamp(): String {
        return format(FORMAT_TIMESTAMP)
    }

    fun format(pattern: String): String {
        return format(longValue, pattern)
    }

    fun formatShort(): String {
        val now = DateTime()
        if (now.year != year) {
            return formatDate()
        }
        if (now.dayOfYear != dayOfYear) {
            return format("M-d")
        }
        return format("H:mm")
    }

    fun formatTemp(): String {
        return format("yyyyMMdd_HHmmss_SSS")
    }

    companion object {
        const val MINUTE_MILL = 60 * 1000
        const val HOUR_MILL = 3600 * 1000

        const val FORMAT_DATE = "yyyy-MM-dd"
        const val FORMAT_DATE8 = "yyyyMMdd"
        const val FORMAT_TIME = "HH:mm:ss"
        const val FORMAT_TIME_X = "HH:mm:ss.SSS"
        const val FORMAT_DATE_TIME = "yyyy-MM-dd HH:mm:ss"
        const val FORMAT_DATE_TIME_NO_SEC = "yyyy-MM-dd HH:mm"
        const val FORMAT_DATE_TIME_X = "yyyy-MM-dd HH:mm:ss.SSS"
        const val FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss.SSS"
        val timeZone0: TimeZone = TimeZone.getTimeZone("GMT+0:00")

        val now: DateTime get() = DateTime()

        //month [1,12]
        fun datetime(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0, millSeconds: Int = 0, utc: Boolean = false): DateTime {
            val zone: TimeZone = if (utc) timeZone0 else TimeZone.getDefault()
            val d = DateTime(0, zone)
            d.year = year
            d.monthX = month
            d.day = day
            d.hour = hour
            d.minute = minute
            d.second = second
            d.millSecond = millSeconds;
            return d
        }

        //month [1,12]
        fun time(hour: Int, minute: Int = 0, second: Int = 0, millSeconds: Int = 0, utc: Boolean = false): DateTime {
            val zone: TimeZone = if (utc) timeZone0 else TimeZone.getDefault()
            val d = DateTime(0, zone)
            d.year = 0
            d.monthX = 0
            d.day = 0
            d.hour = hour
            d.minute = minute
            d.second = second
            d.millSecond = millSeconds;
            return d
        }

        //month [1,12]
        fun date(year: Int, month: Int, day: Int, utc: Boolean = false): DateTime {
            val zone: TimeZone = if (utc) timeZone0 else TimeZone.getDefault()
            val d = DateTime(0, zone)
            d.year = year
            d.monthX = month
            d.day = day
            d.hour = 0
            d.minute = 0
            d.second = 0
            d.millSecond = 0
            return d
        }

        fun from(date: Date): DateTime {
            return DateTime(date.time)
        }

        fun from(date: java.sql.Date): DateTime {
            return DateTime(date.time)
        }

        fun from(date: Time): DateTime {
            return DateTime(date.time)
        }

        fun from(date: Timestamp): DateTime {
            return DateTime(date.time)
        }

        fun from(date: LocalDate): DateTime {
            return date(date.year, date.monthValue, date.dayOfMonth)
        }

        fun from(date: LocalTime): DateTime {
            return time(date.hour, date.minute, date.second, date.nano / 1_000_000)
        }

        fun from(date: LocalDateTime): DateTime {
            return datetime(date.year, date.monthValue, date.dayOfMonth, date.hour, date.minute, date.second, date.nano / 1_000_000)
        }

        fun format(date: Long, pattern: String): String {
            val ff = SimpleDateFormat(pattern, Locale.getDefault())
            return ff.format(Date(date))
        }

        fun parse(formats: List<String>, dateStr: String, locale: Locale = Locale.getDefault()): DateTime? {
            for (f in formats) {
                val d = parse(f, dateStr, locale)
                if (d != null) return d
            }
            return null
        }

        fun parse(format: String, dateStr: String, locale: Locale = Locale.getDefault()): DateTime? {
            val ff = SimpleDateFormat(format, locale)
            try {
                val d = ff.parse(dateStr)
                if (d != null) {
                    return DateTime(d.time)
                }
            } catch (_: Exception) {
            }
            return null
        }

        fun parseDate(s: String?): DateTime? {
            if (s == null || s.length < 6) {
                return null
            }
            return parse(listOf("yyyy-MM-dd", "yyyy-M-d"), s)
        }

        fun parseTime(s: String?): DateTime? {
            if (s == null || s.length < 6) {
                return null
            }
            return parse(listOf("HH:mm:ss", "H:m:s", "HH:mm:ss.SSS", "H:m:s.S"), s)
        }

        fun parseDateTime(s: String?): DateTime? {
            if (s == null || s.length < 6) {
                return null
            }
            return parse(listOf("yyyy-MM-dd HH:mm:ss.SSS", "yyyy-M-d H:m:s.S", "yyyy-MM-dd HH:mm:ss", "yyyy-M-d H:m:s", "yyyy-MM-ddTHH:mm:ss.SSS", "yyyy-M-dTH:m:s.S", "yyyy-MM-ddTHH:mm:ss", "yyyy-M-dTH:m:s"), s)
        }

        fun formatDuration(seconds: Long): String {
            if (seconds < 60) {
                return "${seconds}秒"
            }
            if (seconds < 60 * 60) {
                return "${seconds / 60}分${seconds % 60}秒"
            }
            return "${seconds / 3600}时${seconds % 3600 / 60}分${seconds % 60}秒"
        }
    }
}

