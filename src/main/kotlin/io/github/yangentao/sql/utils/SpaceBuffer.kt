@file:Suppress("unused")

package io.github.yangentao.sql.utils

class SpaceBuffer(capcity: Int = 128) {
    val buffer: StringBuilder = StringBuilder(capcity)

    @Suppress("unused")
    val length: Int get() = buffer.length
    val isEmpty: Boolean get() = buffer.isEmpty()
    val isNotEmpty: Boolean get() = buffer.isNotEmpty()
    val lastChar: Char? get() = buffer.lastOrNull()
    val firstChar: Char? get() = buffer.firstOrNull()

    operator fun rangeTo(text: String): SpaceBuffer {
        if (text.isEmpty()) return this
        if (isEmpty || lastChar == ' ' || text.first() in noSpaces) {
            buffer.append(text)
        } else {
            buffer.append(' ')
            buffer.append(text)
        }
        return this
    }

    operator fun rangeUntil(text: String): SpaceBuffer {
        if (text.isEmpty()) return this
        buffer.append(text)
        return this
    }

    operator fun plusAssign(text: String) {
        this..text
    }

    fun parenthesed(block: () -> Unit): SpaceBuffer {
        this.."("
        block()
        this..")"
        return this
    }

    fun <V> parenthesedAll(items: Collection<V>, sep: String = ",", onItem: ((V) -> Unit)? = null): SpaceBuffer {
        return addEach(items, sep, true, onItem)
    }

    fun <V> addEach(items: Collection<V>, sep: String = ",", parenthesed: Boolean = false, onItem: ((V) -> Unit)? = null): SpaceBuffer {
        if (parenthesed) this.."("
        items.forEachIndexed { n, v ->
            if (n != 0) this..sep
            if (onItem == null) {
                if (v == null) this.."NULL" else this..v.toString()
            } else {
                onItem(v)
            }
        }
        if (parenthesed) this..")"
        return this
    }

    override fun toString(): String {
        return buffer.toString()
    }

    companion object {
        private val noSpaces = setOf(' ', ',', '(', ')')
    }
}


