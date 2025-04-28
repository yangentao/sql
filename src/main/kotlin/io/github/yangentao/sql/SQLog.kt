package io.github.yangentao.sql


typealias SQLLogHandler = (String) -> Unit

object SQLog {
    var ERROR_LOGGER: SQLLogHandler = { println(it) }
    var DEBUG_LOGGER: SQLLogHandler? = null
    fun err(vararg vs: Any?) {
        val s = vs.joinToString(" ") {
            it?.toString() ?: "null"
        }
        ERROR_LOGGER(s)
    }

    fun debug(vararg vs: Any?) {
        val c = DEBUG_LOGGER ?: return
        val s = vs.joinToString(" ") {
            it?.toString() ?: "null"
        }
        c(s)
    }

}