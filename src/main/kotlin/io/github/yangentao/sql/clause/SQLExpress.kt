package io.github.yangentao.sql.clause

import io.github.yangentao.anno.userName
import io.github.yangentao.sql.*
import io.github.yangentao.sql.utils.SpaceBuffer
import java.time.temporal.Temporal
import java.util.*
import kotlin.reflect.KClass

open class SQLExpress(clause: Any? = null, args: ArgList = emptyList()) {
    val buffer: SpaceBuffer = SpaceBuffer()
    val arguments: ArrayList<Any?> = ArrayList()

    init {
        if (clause != null) this..clause
        if (args.isNotEmpty()) arguments.addAll(args)
    }

    val isEmpty: Boolean get() = buffer.isEmpty
    val isNotEmpty: Boolean get() = buffer.isNotEmpty
    val sql: String get() = buffer.toString().trim()

    open fun append(express: Any): SQLExpress {
        when (express) {
            is String -> buffer..express
            is SQLExpress -> {
                buffer..express.sql
                arguments.addAll(express.arguments)
            }

            is Number -> buffer..express.toString()
            is PropSQL -> buffer..express.fullNmeSQL.escapeSQL
            is KClass<*> -> buffer..express.nameSQL.escapeSQL
            is BaseModelClass<*> -> buffer..express.tableClass.nameSQL.escapeSQL
            is List<*> -> this.addList(express.filterNotNull())
            is Set<*> -> this.addList(express.filterNotNull())
            is Array<*> -> this.addList(express.filterNotNull())
            else -> error("SQLExpress not support value: $express ")
        }
        return this
    }

    private fun addList(exps: Collection<Any>, joinString: String = ",") {
        if (exps.isEmpty()) return
        for ((n, item) in exps.withIndex()) {
            if (n != 0) this..joinString
            this..item
        }
    }

    override fun toString(): String {
        return sql
    }
}

infix operator fun <T : SQLExpress> T.rangeTo(express: Any): T {
    this.append(express)
    return this
}

infix operator fun <T : SQLExpress> T.rangeUntil(express: Any): T {
    if (express is String || express is Date || express is Temporal) {
        this.append("?")
        this.arguments.add(express)
    } else {
        this.append(express)
    }
    return this
}

fun <T : SQLExpress> T.parenthesed(express: Any): T {
    this.."("
    this..express
    this..")"
    return this
}

fun <T : SQLExpress, V : Any> T.addEach(items: Collection<V>, sep: Any = ",", parenthesed: Boolean = false, onItem: ((V) -> Unit)? = null): T {
    if (parenthesed) this.."("
    items.forEachIndexed { n, v ->
        if (n != 0) this..sep
        if (onItem == null) {
            this..v
        } else {
            onItem(v)
        }
    }
    if (parenthesed) this..")"
    return this
}

fun <T : SQLExpress, V : Any> T.addEachX(items: Collection<V?>, sep: Any = ",", onItem: (V?) -> Unit): T {
    items.forEachIndexed { n, v ->
        if (n != 0) this..sep
        onItem(v)
    }
    return this
}

class ShortExpress(express: Any) : SQLExpress(express) {
    override fun append(express: Any): SQLExpress {
        if (express is PropSQL) {
            buffer..express.userName.escapeSQL
            return this
        }
        return super.append(express)
    }
}