package io.github.yangentao.sql

import io.github.yangentao.xlog.TagLog

internal val logSQL = TagLog("SQL")

object SQLog {
    fun err(vararg vs: Any?) {
        logSQL.e(*vs)
    }

    fun debug(vararg vs: Any?) {
        logSQL.d(*vs)
    }

}