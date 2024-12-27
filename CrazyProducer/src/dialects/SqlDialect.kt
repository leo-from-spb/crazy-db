package lb.crazy.producer.dialects

import kotlin.math.abs
import kotlin.math.truncate


sealed class SqlDialect {

    abstract val name : String

    open val identifierLengthLimit: Int = 60

    open val commandDelimiter: String = ";"

    open val foreignKeyAlwaysRequiresColumnList: Boolean = false

    open val nativeNumericIsDecimal: Boolean = false

    open fun typeForInt(bytes: Byte): String =
        when (bytes.toInt()) {
            0, 1, 2 -> "smallint"
            3, 4 -> "int"
            5, 6, 7, 8 -> "bigint"
            else -> "number(${truncate(1f + bytes*2.408239f)})"
        }

    open fun typeForDecimalInt(digits: Int): String = "decimal($digits)"

    open fun typeForIntRange(min: Int, max: Int): String {
        val v: Long = kotlin.math.max(abs(min.toLong()), abs(max.toLong()))
        if (nativeNumericIsDecimal) {
            val digits = v.toString().length
            return typeForDecimalInt(digits)
        }
        else {
            return when {
                v <= 127 -> typeForInt(1)
                v <= 32767 -> typeForInt(2)
                v <= 8388607 -> typeForInt(3)
                v <= 2147483647 -> typeForInt(4)
                else -> typeForInt(8)
            }
        }
    }

    open fun typeForBoolean() =
        if (nativeNumericIsDecimal) "decimal(1)"
        else typeForInt(1)

    open fun typeForString(chars: Int) = "varchar($chars)"
    

    override fun toString() = this.javaClass.simpleName
}



class GenericDialect : SqlDialect() {

    override val name = "Generic"
    override val nativeNumericIsDecimal = true

}



class OracleDialect : SqlDialect() {

    override val name = "Oracle"
    override val commandDelimiter: String = "/"
    override val nativeNumericIsDecimal = true

}



class MsDialect : SqlDialect() {

    override val name = "Mssql"

    override val commandDelimiter: String = "go"

}



class MysqlDialect : SqlDialect() {

    override val name = "Mysql"
    override val identifierLengthLimit = 64
    override val foreignKeyAlwaysRequiresColumnList = true
    
}