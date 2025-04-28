@file:Suppress("unused", "ConstPropertyName", "UsePropertyAccessSyntax", "DEPRECATION")

package io.github.yangentao.sql.pool

import io.github.yangentao.reflect.IntervalRun
import io.github.yangentao.sql.SQLog
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

object EntaoPool : NamedConnections {
    private var maxSize: Int = 50
    private const val pickTimeout: Long = 10_000
    private val intervalRun: IntervalRun = IntervalRun(5000)
    private val creatorMap: ConcurrentHashMap<String, ConnectionBuilder> = ConcurrentHashMap()
    private val connectionMap: ConcurrentHashMap<String, ConcurrentLinkedQueue<ConnectionProxy>> = ConcurrentHashMap()
    private val connCount: AtomicInteger = AtomicInteger(0)
    val totalSize: Int get() = connCount.get()
    var logEnable: Boolean = true

    @Synchronized
    override fun push(name: String, builder: ConnectionBuilder) {
        connectionMap.remove(name)?.forEach { it.connection.close() }
        creatorMap.remove(name)
        creatorMap[name] = builder
    }

    //定时5秒， 尝试回收
    override fun pickOne(name: String): Connection {
        val c = pickFrom(name, System.currentTimeMillis())
        intervalRun.run {
            tryRecyle()
        }
        return c
    }

    fun setMaxConnectionCount(maxCount: Int) {
        maxSize = maxCount
    }

    //1。尝试复用
    //2。新建
    //3。回收
    //4。超时失败
    private fun pickFrom(name: String, fromTime: Long): Connection {
        pickReuse(name)?.also { return it.proxy }
        if (connCount.get() < maxSize) {
            return newConnection(name).proxy
        }
        Thread.sleep(10)
        pickReuse(name)?.also { return it.proxy }
//        tryRecyle()
        if (System.currentTimeMillis() - fromTime > pickTimeout) error("Pick Connection Timeout!")
        return pickFrom(name, fromTime)
    }

    private fun newConnection(name: String): ConnectionProxy {
        connCount.incrementAndGet()
        try {
            val creator = creatorMap[name] ?: error("NO JDBC creator named: $name")
            val c = creator.create()
            val pc = ConnectionProxy(c)
            val ls = connectionMap.getOrPut(name) { ConcurrentLinkedQueue() }
            ls.add(pc)
            return pc
        } catch (ex: Exception) {
            connCount.decrementAndGet()
            SQLog.err("Create New Connection Failed.")
            throw ex
        }
    }

    private fun pickReuse(name: String): ConnectionProxy? {
        val existList = connectionMap[name] ?: return null
        if (existList.isEmpty()) return null
        val reuseList = ArrayList<ConnectionProxy>()
        val a = existList.iterator()
        while (a.hasNext()) {
            val pc = a.next()
            if (pc.connection.isClosed) {
                a.remove()
                connCount.decrementAndGet()
                continue
            }
            if (!pc.canReuse) {
                continue
            }
            reuseList.add(pc)
        }
        if (reuseList.isEmpty()) return null
        val tid = Thread.currentThread().id
        val tm = System.currentTimeMillis()
        fun checkConnValid(pc: ConnectionProxy): Boolean {
            if (!pc.checkValid()) {
                pc.connection.close()
                existList.remove(pc)
                connCount.decrementAndGet()
                return false
            }
            if (pc.canReuse) {//再次检查是否可复用; isValid会与服务器交互， 这期间可能被其他线程抢走
                pc.lastOperateTime.set(tm)
                pc.lastThreadID.set(tid)
                return true
            }
            return false
        }
        //优先挑选上次使用的线程
        for (pc in reuseList.filter { it.lastThreadID.get() == tid }) {
            if (checkConnValid(pc)) return pc
        }
        for (pc in reuseList.filter { it.lastThreadID.get() != tid }) {
            if (checkConnValid(pc)) return pc
        }
        return null
    }

    fun tryRecyle() {
        val tm = System.currentTimeMillis()
        val removedList = ArrayList<ConnectionProxy>()
        for (e in connectionMap.entries) {
            val a = e.value.iterator()
            while (a.hasNext()) {
                val p = a.next()
                if (p.canRecyle(tm)) {
                    a.remove()
                    connCount.decrementAndGet()
                    removedList.add(p)
                }
            }
        }
        for (p in removedList) {
            if (!p.connection.isClosed) {
                p.connection.close()
            }
        }
        if (removedList.isNotEmpty()) {
            SQLog.debug("PoolSize: ", totalSize, " Recyled:", removedList.size)
        }
    }

    @Synchronized
    override fun destroy() {
        SQLog.debug("SimplePool.destroy ")
        connectionMap.values.forEach { ls ->
            ls.forEach {
                if (!it.connection.isClosed) {
                    it.connection.close()
                }
            }
        }
        connectionMap.clear()
        connCount.set(0)
        creatorMap.clear()
    }

}
