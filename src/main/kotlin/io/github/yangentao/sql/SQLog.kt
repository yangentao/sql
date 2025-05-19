package io.github.yangentao.sql

import io.github.yangentao.xlog.TagLog

internal val sqlLog = TagLog("SQL")

object SQLog {
    fun err(vararg vs: Any?) {
        sqlLog.e(*vs)
    }

    fun debug(vararg vs: Any?) {
        sqlLog.d(*vs)
    }

}