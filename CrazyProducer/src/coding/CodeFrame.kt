@file:Suppress("nothing_to_inline")

package lb.crazy.producer.coding

/**
 * Coding frame.
 */
class CodeFrame internal constructor (

    private val file: CodeFile,
    private val parent: CodeFrame?,
    private val indent: String? = null

) {

    private val absoluteIndent: String

    init {
        absoluteIndent = (parent?.absoluteIndent ?: "") + (indent ?: "")
    }


    fun frame(indent: Boolean = true, coder: CodeFrame.() -> Unit) {
        val frame = CodeFrame(file, this, if (indent) file.indent else null)
        frame.coder()
    }

    fun frame(indent: String, coder: CodeFrame.() -> Unit) {
        val frame = CodeFrame(file, this, indent)
        frame.coder()
    }


    fun phrase(vararg words: CharSequence?) {
        val n = words.size
        if (n == 0) return
        var k = 0
        var word = words[0]
        while (k < n && word.isNullOrBlank()) word = words[k++]
        if (k == n) return
        file.append(absoluteIndent)
        file.append(word!!.trim())
        k++
        while (k < n) {
            val word = words[k++]
            if (word.isNullOrBlank()) continue
            file.append(' ')
            appendString(word.trim())
        }
        file.appendEoln()
    }

    fun line(string: CharSequence?) {
        if (string != null && string.isNotEmpty()) {
            file.append(absoluteIndent)
            appendString(string, true)
        }
    }

    fun lines(strings: Iterable<CharSequence?>) {
        for (string in strings) line(string)
    }

    private fun appendString(string: CharSequence, eoln: Boolean = false) {
        val lb = string.indexOf('\n')
        if (lb >= 0) throw IllegalArgumentException("The string contains unexpected line breaks:\n$string")
        file.append(string, eoln)
    }

    fun text(text: CharSequence?) {
        if (text.isNullOrBlank()) return
        val n = text.length
        var p1 = 0
        do {
            var p2 = text.indexOf('\n', startIndex = p1)
            if (p2 < 0) p2 = n
            if (p2 > p1) {
                file.append(absoluteIndent)
                file.append(text.subSequence(p1, p2), true)
            }
            p1 = p2 + 1
        } while (p1 < n)
    }

    fun texts(texts: Iterable<CharSequence?>) {
        for (text in texts) this.text(text)
    }

    inline operator fun CharSequence?.unaryPlus(): CodeFrame {
        text(this)
        return this@CodeFrame
    }

    inline operator fun Iterable<CharSequence?>.unaryPlus(): CodeFrame {
        texts(this)
        return this@CodeFrame
    }

    fun emptyLine() {
        file.appendEoln()
    }

    fun emptyLines(n: Int) {
        for (i in 1..n) file.appendEoln()
    }

    fun removeLastChar(char: Char) {
        file.removeLastChar(char)
    }

    fun replaceLastChar(what: Char, with: Char) {
        file.replaceLastChar(what, with)
    }

}