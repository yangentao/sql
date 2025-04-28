@file:Suppress("DEPRECATION", "PrivatePropertyName", "unused")

package io.github.yangentao.sql.pool

import io.github.yangentao.sql.SQLog
import io.github.yangentao.types.ProxyInvoker
import io.github.yangentao.types.invokeInstance
import io.github.yangentao.types.proxyInterface
import java.lang.reflect.Method
import java.sql.Connection
import java.sql.Statement
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class ConnectionProxy(val connection: Connection) : ProxyInvoker {
    val proxy: Connection = proxyInterface<Connection>(this)
    private val statementSet: ConcurrentLinkedQueue<StatementProxy> = ConcurrentLinkedQueue()
    private val executing: AtomicBoolean = AtomicBoolean(false)
    val lastOperateTime: AtomicLong = AtomicLong(System.currentTimeMillis())
    val lastThreadID: AtomicLong = AtomicLong(Thread.currentThread().id)

    //    private val autoCommit: Boolean get() = originalObject.autoCommit
    private val transacting: Boolean get() = !connection.autoCommit

    private val VALID_INTERVAL_MILL: Long = 3000L
    private val validTime: AtomicLong = AtomicLong(0L)
    private val validResult: AtomicBoolean = AtomicBoolean(true)

    private val STATEMENT_ALIVE_MILL: Long = 10_000L

    override fun invoke(method: Method, args: Array<out Any?>?): Any? {
        if (method.name in RECORD_SET) {
            val tid = Thread.currentThread().id
            lastThreadID.set(tid)
            lastOperateTime.set(System.currentTimeMillis())
            executing.set(true)
            try {
                val result = method.invokeInstance(connection, args)
                return if (method.name in CREATE_STATEMENTS) addStatement(result as Statement) else result
            } finally {
                executing.set(false)
                lastThreadID.set(tid)
                lastOperateTime.set(System.currentTimeMillis())
            }
        }
//        if (method.name == CLOSE) {
//            if (!originalObject.isClosed) {
//            }
//        }
        return method.invokeInstance(connection, args)
    }

    fun checkValid(): Boolean {
        val tm = System.currentTimeMillis()
        if (tm - validTime.get() < VALID_INTERVAL_MILL) return validResult.get()
        val b = connection.isValid(5)
        validResult.set(b)
        validTime.set(tm)
        return b
    }

    fun canRecyle(tm: Long = System.currentTimeMillis()): Boolean {
        if (connection.isClosed) return true
        if (transacting) return false
        if (executing.get()) return false
        if (tm - lastOperateTime.get() < 3_000) return false
        if (statementSet.isNotEmpty()) {
            if (statementSet.any { it.executing.get() }) return false
            statementSet.maxByOrNull { it.lastExecuteTime.get() }?.also { st ->
                if (st.lastExecuteTime.get() == 0L || (tm - st.lastExecuteTime.get() < STATEMENT_ALIVE_MILL)) {
                    return false
                } else {
                    SQLog.err("Statement maybe NOT be closed")
                }
            }
        }
//        if (!statementSet.isEmpty()) {
//            loge("Statement maybe NOT be closed")
//            return false
//        }
        return true
    }

    val canReuse: Boolean
        get() {
            val currentTid = Thread.currentThread().id
            if (currentTid == lastThreadID.get()) {
                return true
            }
            if (transacting) return false
//            return statementSet.isEmpty()
            return !executing.get()

        }

    private fun addStatement(st: Statement): Statement {
        val sp = StatementProxy(this, st)
        statementSet.add(sp)
        return sp.proxy
    }

    fun removeStatement(statementProxy: StatementProxy) {
        statementSet.remove(statementProxy)
    }

    fun statementExecuteStart(method: Method, threadId: Long, startTime: Long) {
        executing.set(true)
        lastThreadID.set(threadId)
        lastOperateTime.set(startTime)
    }

    fun statementExecuteEnd(method: Method, threadId: Long, startTime: Long, endTime: Long) {
        executing.set(false)
        lastThreadID.set(threadId)
        lastOperateTime.set(endTime)
        val delta = endTime - startTime
        if (delta > 1_000) {
            SQLog.debug("WARING: execute ${method.name} time:", delta)
        }
    }

    companion object {
        val RECORD_SET: Set<String> = setOf(
            "createStatement", "prepareStatement", "prepareCall", "setAutoCommit", "commit", "rollback",
            "setReadOnly", "setCatalog", "setTransactionIsolation", "setHoldability", "setSavepoint", "releaseSavepoint", "beginRequest", "endRequest"
        )
        val CREATE_STATEMENTS: Set<String> = setOf("createStatement", "prepareStatement", "prepareCall")
        const val CLOSE: String = "close"
    }
}
