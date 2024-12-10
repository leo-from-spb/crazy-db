@file:JvmName("StringFun")

package lb.crazy.model


fun String.wrap(prefix: Char, suffix: Char): String = prefix + this + suffix


