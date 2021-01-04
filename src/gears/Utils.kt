@file:Suppress("nothing_to_inline")

package lb.crazydb.gears

import java.util.*
import kotlin.system.exitProcess



fun emptyNameSet(): MutableSet<String> = TreeSet(String.CASE_INSENSITIVE_ORDER)

fun newNameSet(vararg names: String): MutableSet<String> = emptyNameSet().apply { addAll(names) }



fun wordSetOf(vararg words: String): Set<String> {
    val set = TreeSet(String.CASE_INSENSITIVE_ORDER)
    set.addAll(words.asList())
    return set
}



fun say(message: String) {
    println(message)
}



fun panic(message: String): Nothing {
    System.err.println(message)
    System.err.println()
    exitProcess(100)
}


fun StringBuilder.phrase(vararg parts: CharSequence?): StringBuilder {
    var was = false
    for (part in parts) {
        if (part.isNullOrBlank()) continue
        if (was) space()
        append(part)
        was = true
    }
    return this
}


fun StringBuilder.comma(): StringBuilder = append(',')
fun StringBuilder.space(): StringBuilder = append(' ')
fun StringBuilder.tab():   StringBuilder = append('\t')
fun StringBuilder.eoln():  StringBuilder = append('\n')

fun StringBuilder.eolnIfNo(): StringBuilder {
    if (!endsWith('\n')) append('\n')
    return this
}

fun StringBuilder.space(w: Int = 1): StringBuilder {
    for (i in 1..w) append(' ')
    return this
}

fun StringBuilder.removeEnding(char: Char = ','): StringBuilder {
    if (endsWith(char)) deleteCharAt(length-1)
    else if (endsWith("$char\n")) deleteCharAt(length-2)
    return this
}


inline infix fun Boolean.then(string: String): String? = if (this) string else null
inline infix fun<T> Boolean.then(producer: () -> T): T? = if (this) producer() else null


fun Random.nextChar(min: Char, max: Char): Char {
    val n: Int = max.toInt() - min.toInt() + 1
    val x = this.nextInt(n)
    val c: Char = min + x
    return c
}



infix fun<T> T.change(chg: Pair<T,T>): T = if (this == chg.first) chg.second else this


operator fun Array<out String>.plus(s: String?): Array<out String> {
    if (s == null) return this
    if (this.isEmpty()) return arrayOf(s)
    val n = this.size
    @Suppress("unchecked_cast")
    val array: Array<String> = arrayOfNulls<String>(n + 1) as Array<String>
    System.arraycopy(this, 0, array, 0, n)
    array[n] = s
    return array
}


fun String.tabs() = this.replace('¬', '\t')


infix fun CharSequence.shiftTextWith(prefix: Char): CharSequence =
    when {
        this.isEmpty() -> this
        '\n' !in this -> this
        this.last() == '\n' -> prefix + this.subSequence(0, length-1).replace(eolnPattern, "\n$prefix") + ' '
        else -> prefix + this.replace(eolnPattern, "\n$prefix")
    }

infix fun CharSequence.shiftTextBodyWith(prefix: String): CharSequence =
    when {
        this.isEmpty() -> this
        '\n' !in this -> this
        this.last() == '\n' -> this.subSequence(0, length-1).replace(eolnPattern, "\n$prefix") + ' '
        else -> this.replace(eolnPattern, "\n$prefix")
    }


private val eolnPattern = Regex("""\n""")


fun Array<String>.parenthesized(): String = joinToString(prefix = "(", postfix = ")")
fun Collection<String>.parenthesized(): String = joinToString(prefix = "(", postfix = ")")

fun<T> Array<T>.parenthesized(transform: (T) -> CharSequence): String = joinToString(prefix = "(", postfix = ")", transform = transform)
fun<T> Collection<T>.parenthesized(transform: (T) -> CharSequence): String = joinToString(prefix = "(", postfix = ")", transform = transform)


fun Random.nextInt(min: Int, max: Int): Int {
    val bound = max - min + 1
    val x = this.nextInt(bound)
    return min + x
}


val nop: Nothing? inline get() = null
