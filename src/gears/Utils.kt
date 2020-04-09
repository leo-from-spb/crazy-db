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


fun StringBuilder.phrase(vararg parts: String?): StringBuilder {
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

fun StringBuilder.removeEnding(char: Char = ','): StringBuilder {
    if (endsWith(char)) deleteCharAt(length-1)
    else if (endsWith("$char\n")) deleteCharAt(length-2)
    return this
}


fun Boolean.then(string: String): String? = if (this) string else null
