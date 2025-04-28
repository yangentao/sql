@file:Suppress("DEPRECATION")

package io.github.yangentao.sql.pool

import io.github.yangentao.reflect.ProxyInvoker
import io.github.yangentao.reflect.invokeInstance
import io.github.yangentao.reflect.proxyInterface
import java.lang.reflect.Method
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class StatementProxy(private val proxyConnection: ConnectionProxy, val statement: Statement) : ProxyInvoker {
    val proxy: Statement = if (statement is PreparedStatement) proxyInterface<PreparedStatement>(this) else proxyInterface<Statement>(this)

    val executing: AtomicBoolean = AtomicBoolean(false)
    val lastExecuteTime: AtomicLong = AtomicLong(System.currentTimeMillis())

    override fun invoke(method: Method, args: Array<out Any?>?): Any? {
        when (method.name) {
            GET_CONNECTION -> return proxyConnection.proxy
            CLOSE -> try {
                return method.invokeInstance(statement, args)
            } finally {
                proxyConnection.removeStatement(this)
            }

            else -> if (method.name.startsWith(EXECUTE)) {
                executing.set(true)
                val tid = Thread.currentThread().id
                val startTime = System.currentTimeMillis()
                lastExecuteTime.set(startTime)
                proxyConnection.statementExecuteStart(method, tid, startTime)
                try {
                    try {
                        return checkResultSet(method.invokeInstance(statement, args))
                    } catch (ex: Throwable) {
                        try {
                            this.statement.close()
                        } finally {
                            proxyConnection.removeStatement(this)
                        }
                        throw ex
                    }
                } finally {
                    val endtime = System.currentTimeMillis()
                    executing.set(false)
                    lastExecuteTime.set(endtime)
                    proxyConnection.statementExecuteEnd(method, tid, startTime, endtime)
                }
            } else {
                return checkResultSet(method.invokeInstance(statement, args))
            }
        }
    }

    private fun checkResultSet(v: Any?): Any? {
        if (v is ResultSet) {
            return ResultSetProxy(this, v).proxy
        }
        return v
    }

    companion object {
        private const val CLOSE: String = "close"
        private const val EXECUTE: String = "execute"
        private const val GET_CONNECTION: String = "getConnection"
    }
}