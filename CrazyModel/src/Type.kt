package lb.crazy.model

/**
 * Abstract data type.
 */
sealed class Type 


object BoolType : Type() {
    override fun toString() = "Bool"
}


sealed class NumericType : Type()


class IntType private constructor (val bytes: Byte) : NumericType() {

    companion object {
        val int1 = IntType(1)
        val int2 = IntType(2)
        val int4 = IntType(4)
        val int8 = IntType(8)
        val tiny  = int1
        val short = int2
        val norm  = int4
        val long  = int8
    }

    override fun toString() = "int$bytes"
}


class DecimalIntType (val digits: Int) : NumericType() {
    override fun toString() = "decimal(digits)"
}


class RangeIntType (val min: Int, val max: Int) : NumericType() {
    override fun toString() = "min..max"
}



class StrType (val size: Int) : Type() {
    override fun toString() = "Str($size)"
}



