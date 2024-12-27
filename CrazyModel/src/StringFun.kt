@file:JvmName("StringFun")

package lb.crazy.model


fun String.wrap(prefix: Char, suffix: Char): String = prefix + this + suffix


fun String.withPrefix(prefix: String?, delimiter: Char): String =
    if (prefix != null && prefix.isNotEmpty()) "$prefix$delimiter$this"
    else this


fun String.left(len: Int): String =
    if (this.length <= len) this
    else this.substring(0, len)

fun String.right(len: Int): String =
    if (this.length <= len) this
    else this.substring(this.length - len)


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


val String.inParens: String get() = "($this)"



@JvmName("adjustIdentifierBySizeN")
fun adjustIdentifierBySize(identifierWithoutSuffix: String?, suffix: String? = null, limit: Int): String? =
    if (identifierWithoutSuffix != null) adjustIdentifierBySize(identifierWithoutSuffix, suffix, limit) else null

fun adjustIdentifierBySize(identifierWithoutSuffix: String, suffix: String? = null, limit: Int): String {
    val m = if (suffix == null) limit else limit - suffix.length - 1
    assert(m > 0)
    var s = identifierWithoutSuffix.left(m)
    if (suffix != null) s = s + '_' + suffix
    return s
}
