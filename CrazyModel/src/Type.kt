package lb.crazy.model

/**
 * Abstract data type.
 */
sealed class Type 


object BoolType : Type() {

    override fun toString() = "Bool"

}


class NumType (val range: IntRange?) : Type() {

    override fun toString() = "Num" + (range?.toString()?.wrap('(',')') ?: "")

}

class StrType (val size: Int) : Type() {

    override fun toString() = "Str($size)"

}



