@file:Suppress("unused")

package io.github.yangentao.reflect

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy

fun interface ProxyInvoker {
    fun invoke(method: Method, args: Array<out Any?>?): Any?
}

inline fun <reified I> proxyInterface(invoker: ProxyInvoker): I {
    val cls = I::class.java
    if (!cls.isInterface) {
        error("proxyInterface() must has an interface , not object.")
    }
    return Proxy.newProxyInstance(cls.classLoader, arrayOf(cls), InvocationHandlerDelegage(invoker)) as I
}

class InvocationHandlerDelegage(val invoker: ProxyInvoker) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        return invoker.invoke(method, args)
    }
}

fun Method.invokeInstance(inst: Any, args: Array<out Any?>?): Any? {
    if (args == null) {
        return this.invoke(inst)
    }
    return this.invoke(inst, *args)
}