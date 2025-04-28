package io.github.yangentao.sql

import io.github.yangentao.reflect.decodeValueTyped
import java.sql.Array
import kotlin.reflect.KProperty

inline fun <reified T : Any> Array.toList(prop: KProperty<T>): List<T> {
    return this.toList { row ->
        val v = row.objectValue(2)
        prop.decodeValueTyped(v) as T
    }
}

fun <T : Any> Array.toList(block: (ResultRow) -> T): List<T> {
    val ls = this.resultSet.map(block)
    this.free()
    return ls
}

val Array.listBool: List<Boolean> get() = this.toList { it.boolValue(2)!! }
val Array.listByte: List<Byte> get() = this.toList { it.resultSet.getByte(2) }
val Array.listShort: List<Short> get() = this.toList { it.resultSet.getShort(2) }
val Array.listInt: List<Int> get() = this.toList { it.resultSet.getInt(2) }
val Array.listLong: List<Long> get() = this.toList { it.resultSet.getLong(2) }
val Array.listFloat: List<Float> get() = this.toList { it.resultSet.getFloat(2) }
val Array.listDouble: List<Double> get() = this.toList { it.resultSet.getDouble(2) }
val Array.listString: List<String> get() = this.toList { it.resultSet.getString(2) }
val Array.listChar: List<Char> get() = this.toList { it.resultSet.getString(2).first() }
val Array.listObject: List<Any?> get() = this.toList { it.resultSet.getObject(2) }