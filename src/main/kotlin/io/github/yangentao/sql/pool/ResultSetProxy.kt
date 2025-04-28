package io.github.yangentao.sql.pool

import io.github.yangentao.types.ProxyInvoker
import io.github.yangentao.types.invokeInstance
import io.github.yangentao.types.proxyInterface
import java.lang.reflect.Method
import java.sql.ResultSet

//ResultSet.close()调用时， 调用了Statement.close(), 这样导致Connection的Statement被删除， Connection变成可复用状态
class ResultSetProxy(private val proxyStatement: StatementProxy, val resultSet: ResultSet) : ProxyInvoker {
    val proxy: ResultSet = proxyInterface(this)
    override fun invoke(method: Method, args: Array<out Any?>?): Any? {
        return when (method.name) {
            GET_STATEMENT -> proxyStatement.proxy
            CLOSE -> proxyStatement.proxy.close()
            else -> method.invokeInstance(resultSet, args)
        }
    }

    companion object {
        private const val GET_STATEMENT: String = "getStatement"
        private const val CLOSE: String = "close"
    }
}