package lb.crazy.model

/**
 * Abstract data type.
 */
sealed class Type 


object BoolType : Type() {

    override fun toString() = "Bool"

}


class NumType : Type {

    val digits: Int
    val sizeModifier: Byte
    val range: IntRange?
    val positiveOnly: Boolean


    constructor(digits: Int, positiveOnly: Boolean = false) : super() {
        this.digits = digits
        this.sizeModifier =
            when {
                digits <= 3 -> sizeShort
                digits <= 9 -> sizeNorm
                else -> sizeLong
            }
        this.range = null
        this.positiveOnly = positiveOnly
    }

    constructor(sizeModifier: Byte = sizeNorm, positiveOnly: Boolean = false) : super() {
        this.sizeModifier = sizeModifier
        this.digits =
            when (sizeModifier) {
                sizeShort -> 5
                sizeNorm  -> 10
                sizeLong  -> 19
                else -> 19
            }
        this.range = null
        this.positiveOnly = positiveOnly
    }

    constructor(range: IntRange) : super() {
        val wing = range.maxWing
        this.range = range
        this.digits = wing.numberOfDigits
        this.sizeModifier =
            when {
                wing <= 255 -> sizeShort
                wing <= 65535 -> sizeNorm
                else -> sizeLong
            }
        this.positiveOnly = range.first > 0
    }

    override fun toString() = "Num" + (range?.toString()?.wrap('(', ')') ?: "")

}

const val sizeShort: Byte = -1
const val sizeNorm: Byte = 0
const val sizeLong: Byte = +1



class StrType (val size: Int) : Type() {

    override fun toString() = "Str($size)"

}



