package lb.crazydb.schema1

import lb.crazydb.Column
import lb.crazydb.Model
import lb.crazydb.Table
import lb.crazydb.gears.*
import java.util.*


class Contriver(val model: Model, val dict: Dictionary) {

    val usedNames get() = model.usedNames


    val specialWords: Set<String> = wordSetOf("ID", "NR", "ORDER_NR")

    val excludedMinorWords: Set<String>

    val rnd = Random(System.nanoTime() * 17L)


    init {
        excludedMinorWords = TreeSet()
        excludedMinorWords += specialWords
        excludedMinorWords += ReservedWords.words

        usedNames += specialWords
    }


    fun inventPortions(number: Int) {
        for (i in 1 .. number) inventPortion(i)
    }

    private fun inventPortion(portionIndex: Int) {
        val mainWord = dict.guessNoun(6, usedNames)
        val mainAbb = mainWord.abb(4)
        if (mainAbb.length < 2 || mainAbb in usedNames) return

        val mainTable = Table(mainWord)
        val keyColumn = Column(mainTable, "id").apply {
            mandatory = true
            primary = true
            dataType = guessPrimaryDataType()
        }

        val columnsNumber = 3 + rnd.nextInt(30)
        val exceptColumnNames = newNameSet(mainAbb)
        for (k in 1..columnsNumber) {
            val cName = dict.guessNoun(5, exceptColumnNames, excludedMinorWords)
            val column = Column(mainTable, cName).apply {
                mandatory = rnd.nextBoolean() && rnd.nextBoolean()
                dataType = guessSimpleDataType()
            }
            exceptColumnNames += cName
        }

        model.tables += mainTable
    }


    private fun guessSimpleDataType(): String =
        when (rnd.nextInt(9)) {
            0    -> "char"
            1    -> "char(${2 + rnd.nextInt(7)})"
            2    -> "varchar(${10 + rnd.nextInt(100) * 10})"
            3    -> "nvarchar2(${10 + rnd.nextInt(100) * 10})"
            4    -> "number(${1 + rnd.nextInt(4)})"
            5    -> "number(${1 + rnd.nextInt(5)})"
            6    -> "number(${1 + rnd.nextInt(6)})"
            7    -> "number(${1 + rnd.nextInt(7)})"
            8    -> "date"
            else -> "char"
        }

    private fun guessPrimaryDataType(): String =
        when (rnd.nextInt(7)) {
            0    -> "char(${2 + rnd.nextInt(3)})"
            1    -> "varchar(${3 + rnd.nextInt(14)})"
            2    -> "number(4)"
            3    -> "number(18)"
            else -> "number(9)"
        }

}