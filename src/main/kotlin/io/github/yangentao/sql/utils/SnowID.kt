@file:Suppress("unused")

package io.github.yangentao.sql.utils

import io.github.yangentao.types.DateTime
import io.github.yangentao.types.printX
import java.util.concurrent.atomic.AtomicLong

//101101 10001111 11101010 01100100 01000111 11001001 00010000 00000001
//101101 10001111 11101010 01100100 01001010 11001001 00010000 00000010
//101101 10001111 11101010 01100100 01001010 11001001 00010000 00000011
//101101 10001111 11101010 01100100 01001010 11001001 00010000 00000100

private fun testSnowJS() {
    val a: Long = SnowJS.next()
    printX(a.toString(), "  ", a.toString(2), "   ", a.toString(2).length)
    printX(SnowJS.inst.timeOf(a).formatDateTime())
    printX(SnowJS.inst.mchineOf(a))
    printX(SnowJS.inst.seqOf(a))
    printX()
}

private const val JS_MAX_INT: Long = 9007199254740991L  // 53bit 11111111111111111111111111111111111111111111111111111

// 兼容javascript中, js整数最大 2^53-1
// 1符号位(值0), 31时间戳(秒), 4机器码, 18序列号
class SnowJS(private val machine: Long = 1, fromYear: Int = 2020, machineBits: Int = 4) {
    private val seqBits: Int = 22 - machineBits;
    private val machMask: Long = (1L shl machineBits) - 1
    private val seqMask: Long = (1L shl seqBits) - 1
    private val fromTime: Long = DateTime.date(fromYear, 1, 1).timeInMillis
    private var preId: Long = 0

    init {
        assert(machineBits in 1..12)
    }

    fun timeOf(id: Long): DateTime {
        val tm = (id shr 22)
        return DateTime(tm * 1000 + fromTime)
    }

    fun mchineOf(id: Long): Long {
        return (id shr seqBits) and machMask
    }

    fun seqOf(id: Long): Long {
        return id and seqMask
    }

    fun next(): Long {
        var seq = seqLong.getAndAdd(1)
        if (seq >= seqMask) {
            seqLong.set(1)
            seq = seqLong.getAndAdd(1)
        }
        val tm: Long = (System.currentTimeMillis() - fromTime) / 1000
        val id: Long = ((tm and TM_MASK) shl 22) or ((machine and machMask) shl seqBits) or (seq and seqMask)
        if (id > preId) {
            preId = id
            return id
        }
        Thread.sleep(1)
        return next()
    }

    companion object {
        private const val TM_MASK: Long = 0B01111111_11111111_11111111_11111111

        private var seqLong = AtomicLong(1)
        var inst: SnowJS = SnowJS()

        fun next(): Long = inst.next()
    }
}

// javascript中, 会溢出!
// 1符号位(值0), 41时间戳(毫秒), 10机器码, 12序列号
@Deprecated("Use Snow instead.")
class SnowID(private val machine: Long = 1, fromYear: Int = 2020) {
    private val fromTime: Long = DateTime.date(fromYear, 1, 1).timeInMillis
    private var preId: Long = 0

    fun timeOf(id: Long): DateTime {
        val tm = (id shr 22) and TM_MASK
        return DateTime(tm + fromTime)
    }

    fun mchineOf(id: Long): Long {
        return (id shr 12) and MCH_MASK
    }

    fun seqOf(id: Long): Long {
        return id and SEQ_MASK
    }

    fun next(): Long {
        var seq = seqLong.getAndAdd(1)
        if (seq >= 4096) {
            seqLong.set(1)
            seq = seqLong.getAndAdd(1)
        }
        val tm = System.currentTimeMillis() - fromTime
        val id = ((tm and TM_MASK) shl 22) or ((machine and MCH_MASK) shl 12) or (seq and SEQ_MASK)
        if (id > preId) {
            preId = id
            return id
        }
        Thread.sleep(0)//sleep(1)
        return next()
    }

    companion object {
        private const val TM_MASK: Long = 0B01_11111111_11111111_11111111_11111111_11111111
        private const val MCH_MASK: Long = 0B011_11111111
        private const val SEQ_MASK: Long = 0B01111_11111111
        private var seqLong = AtomicLong(1)

        @Suppress("DEPRECATION")
        var inst: SnowID = SnowID()

        fun next(): Long = inst.next()
    }
}
