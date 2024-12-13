@file:JvmName("StringFun")

package lb.crazy.model


fun String.wrap(prefix: Char, suffix: Char): String = prefix + this + suffix


fun String.withPrefix(prefix: String?, delimiter: Char): String =
    if (prefix != null && prefix.isNotEmpty()) "$prefix$delimiter$this"
    else this



/**
 * The last character,
 * or the zero char when this is nukll or empty.
 */
val CharSequence?.lastChar: Char
    get() =
        if (this.isNullOrEmpty()) '\u0000'
        else this[this.length-1]


fun StringBuilder.eoln(): StringBuilder =
    this.append('\n')

fun StringBuilder.eolnIfNo(): StringBuilder {
    if (this.lastChar != '\n') this.append('\n')
    return this
}
