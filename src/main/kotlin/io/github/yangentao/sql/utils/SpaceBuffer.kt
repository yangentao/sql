package io.github.yangentao.sql.utils

class SpaceBuffer(capcity: Int = 256) {
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

    fun bracketed(force: Boolean = false): SpaceBuffer {
        return enclosed('[', ']', force)
    }

    fun braced(force: Boolean = false): SpaceBuffer {
        return enclosed('{', '}', force)
    }

    fun parenthesed(force: Boolean = false): SpaceBuffer {
        return enclosed('(', ')', force)
    }

    fun enclosed(begin: Char, end: Char, force: Boolean = false): SpaceBuffer {
        if (!force) {
            if (this.firstChar == begin && lastChar == end) return this
        }
        this.buffer.insert(0, begin)
        this.buffer.append(end)
        return this
    }

    fun enclosed(chars: EnclosePair, force: Boolean = false): SpaceBuffer {
        return enclosed(chars.begin, chars.end, force)
    }

    override fun toString(): String {
        return buffer.toString()
    }

    companion object {
        private val noSpaces = setOf(' ', ',', '(', ')')
    }
}

data class EnclosePair(val begin: Char, val end: Char) {
    companion object {
        val parentheses = EnclosePair('(', ')')
        val brackets = EnclosePair('[', ']')
        val braces = EnclosePair('{', '}')
    }
}

