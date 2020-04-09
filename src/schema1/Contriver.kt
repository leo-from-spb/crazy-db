package lb.crazydb.schema1

import lb.crazydb.*
import lb.crazydb.TableRole.roleCategory
import lb.crazydb.TableRole.roleMain
import lb.crazydb.TriggerEvent.trigOnInsert
import lb.crazydb.TriggerIncidence.trigBefore
import lb.crazydb.gears.*
import java.util.*
import kotlin.collections.ArrayList


class Contriver(val model: Model, val dict: Dictionary) {

    val usedNames get() = model.usedNames


    val specialWords: Set<String> = wordSetOf("ID", "NR", "ORDER_NR")

    val excludedMinorWords: Set<String>

    val rnd = Random(System.nanoTime() * 17L)


    init {
        excludedMinorWords = emptyNameSet()
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

        val mainSequence = Sequence(mainWord, "seq")
        model.sequences += mainSequence
        usedNames += mainSequence.name

        val mainTable = Table(roleMain, mainWord)
        mainTable.associatedSequence = mainSequence

        val keyColumn = Column(mainTable, "id").apply {
            mandatory = true
            primary = true
            dataType = guessPrimaryDataType()
        }

        val keyCheck = Check(mainTable, mainWord, keyColumn.name, "ch").apply {
            predicate = keyColumn.name + " > 0"
        }

        val trigger = Trigger(mainTable, *(mainTable.nameWords + "id" + "trg")).apply {
            incidence = trigBefore
            event = trigOnInsert
            forEachRow = true
            condition = "new.id is null"
            body = """|select ${mainSequence.name}.nextval
                      |into :new.${keyColumn.name}
                      |from dual;
                   """.trimMargin()
        }

        val inheritedTableNumber = rnd.nextInt(6).change(1 to 0)

        val columnsNumber =
            if (inheritedTableNumber > 0) 1 + rnd.nextInt(7)
            else 3 + rnd.nextInt(30)
        val exceptColumnNames = newNameSet(mainAbb)
        populateTableWithColumns(mainTable, columnsNumber, exceptColumnNames)
        model.tables += mainTable
        usedNames += mainWord
        usedNames += mainAbb
        usedNames += keyCheck.name
        usedNames += trigger.name

        for (k in 1..inheritedTableNumber) {
            val adjective = dict.guessAdjective(3, usedNames, exceptColumnNames)
            val catTable = Table(roleCategory, adjective, mainWord)
            val catKeyColumn = keyColumn.copyTo(catTable).apply { mandatory = true; primary = true }
            val reference = Reference(catTable, mainTable, *(catTable.nameWords + "fk"))
            reference.domesticColumns = arrayOf(catKeyColumn)
            reference.foreignColumns = arrayOf(keyColumn)
            reference.cascade = true
            catTable.references += reference
            populateTableWithColumns(catTable, 1 + rnd.nextInt(26), exceptColumnNames)
            model.tables += catTable
            usedNames += catTable.name
            usedNames += reference.name
        }
    }

    private fun populateTableWithColumns(table: Table, columnsNumber: Int, exceptColumnNames: MutableSet<String>) {
        val indexColumns: MutableList<Column>? = rnd.nextBoolean ().then { ArrayList<Column>() }
        for (k in 1..columnsNumber) {
            val cName = dict.guessNoun(5, exceptColumnNames, excludedMinorWords)
            val column = Column(table, cName).apply {
                dataType = guessSimpleDataType()
                mandatory = "default" in dataType || rnd.nextBoolean() && rnd.nextBoolean()
            }
            exceptColumnNames += cName
            if (indexColumns != null && rnd.nextInt(2) == 0 && "number" in column.dataType) {
                indexColumns.add(column)
            }
        }
        if (indexColumns != null && indexColumns.isNotEmpty()) {
            val indexNameWords =
                if (indexColumns.size == 1) table.nameWords + indexColumns.first().name + "i"
                else table.nameWords + "x" + "i"
            Index(table, *indexNameWords).apply {
                unique = rnd.nextBoolean() && rnd.nextBoolean()
                columns = indexColumns
            }
        }
    }


    private fun guessSimpleDataType(): String =
        when (rnd.nextInt(12)) {
            0    -> "char"
            1    -> "char default ('${rnd.nextChar('A','Z')}')"
            2    -> "nchar(${2 + rnd.nextInt(7)})"
            3    -> "varchar(${10 + rnd.nextInt(100) * 10})"
            4    -> "nvarchar2(${40 + rnd.nextInt(10) * 40})"
            5    -> "number(${1 + rnd.nextInt(4)}) default (0)"
            6    -> "number(${1 + rnd.nextInt(5)}) default (-1)"
            7    -> "number(${1 + rnd.nextInt(6)})"
            8    -> "number(${1 + rnd.nextInt(7)})"
            9    -> "date"
            10   -> "date default (trunc(sysdate))"
            11   -> "date default (sysdate)"
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