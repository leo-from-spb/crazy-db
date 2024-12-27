package lb.crazy.producer.coding

import lb.crazy.model.eoln
import lb.crazy.model.lastChar

/**
 * In-memory coding file.
 */
class CodeFile {

    /// FILE NAME \\\

    val fileName: String


    /// SETTINGS \\\

    var indent: String = "\t"

    var glueToChars: Set<Char> = setOf(' ', '\t', '(', '.')
    var gluingChars: Set<Char> = setOf(':', ',', ';', '.', ')')


    /// INTERNAL STATE \\\

    private val buff: StringBuilder


    /// CONSTRUCTOR \\\

    constructor(fileName: String) {
        this.fileName = fileName
        buff = StringBuilder(256)
    }


    /// INTERFACE \\\

    fun coding (coder: CodeFrame.() -> Unit) {
        val frame = CodeFrame(this, null)
        frame.coder()
    }

    fun getText(): CharSequence {
        return buff
    }


    /// INTERNAL IMPLEMENTATION \\\

    internal fun append(char: Char) {
        buff.append(char)
    }

    internal fun append(string: CharSequence) {
        buff.append(string)
    }

    internal fun append(string: CharSequence, eoln: Boolean) {
        append(string)
        if (eoln && buff.lastChar != '\n') appendEoln()
    }

    internal fun appendEoln() {
        buff.eoln()
    }

    fun removeLastChar(char: Char) {
        var k = buff.length
        while (k > 0 && buff[k-1].isWhitespace()) k--
        if (buff[k-1] == char) buff.deleteCharAt(k - 1)
    }

    fun replaceLastChar(what: Char, with: Char) {
        var k = buff.length
        while (k > 0 && buff[k-1].isWhitespace()) k--
        if (buff[k-1] == what) buff[k - 1] = with
    }

    val atLineBegin: Boolean
        get() = buff.isEmpty() || lastChar == '\n'

    val lastChar: Char
        get() {
            val n = buff.length
            return if (n == 0) '\u0000' else buff[n-1]
        }



    /// OTHER \\\

    override fun toString() = "CodeFile $fileName (${buff.length} chars)"

}