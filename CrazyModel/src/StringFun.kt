@file:JvmName("StringFun")

package lb.crazy.model


fun String.wrap(prefix: Char, suffix: Char): String = prefix + this + suffix


fun String.withPrefix(prefix: String?, delimiter: Char): String =
    if (prefix != null && prefix.isNotEmpty()) "$prefix$delimiter$this"
    else this

