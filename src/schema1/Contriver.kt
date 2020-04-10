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

        val inheritance = rnd.nextBoolean()
        val inheritedTableNumber = if (inheritance) 2 + rnd.nextInt(6) else 0

        val mainTable = Table(roleMain, mainWord)
        mainTable.associatedSequence = mainSequence

        val exceptColumnNames = newNameSet(mainAbb)

        val keyColumn = TableColumn(mainTable, "id").apply {
            mandatory = true
            primary = true
            setDataTypeAndDefault(guessPrimaryDataType())
            exceptColumnNames += name
        }

        if (!inheritance && rnd.nextBoolean()) {
            val name = when (rnd.nextInt(4)) {
                1    -> "option_id"
                2    -> "type_id"
                3    -> "scope"
                else -> "code"
            }
            TableColumn(mainTable, name).apply {
                dataType = when (rnd.nextInt(3)) {
                    1    -> "char(1)"
                    2    -> "char(2)"
                    else -> "number(1)"
                }
                mandatory = rnd.nextBoolean() || rnd.nextBoolean()
            }
            exceptColumnNames += name
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

        val columnsNumber =
            if (inheritance) 1 + rnd.nextInt(7)
            else 3 + rnd.nextInt(30)
        populateTableWithColumns(mainTable, columnsNumber, exceptColumnNames)
        model.tables += mainTable
        usedNames += mainWord
        usedNames += mainAbb
        usedNames += keyCheck.name
        usedNames += trigger.name

        for (k in 1..inheritedTableNumber) {
            val adjective = dict.guessAdjective(3, usedNames, exceptColumnNames)
            val catTable = Table(roleCategory, adjective, mainWord)
            val catKeyColumn = keyColumn.copyToTable(catTable).apply { mandatory = true; primary = true }
            val reference = Reference(catTable, mainTable, *(catTable.nameWords + "fk"))
            reference.domesticColumns = arrayOf(catKeyColumn)
            reference.foreignColumns = arrayOf(keyColumn)
            reference.cascade = true
            catTable.references += reference
            populateTableWithColumns(catTable, 1 + rnd.nextInt(26), exceptColumnNames)
            model.tables += catTable
            usedNames += catTable.name
            usedNames += reference.name
            mainTable.inheritedTables += catTable
        }

        if (inheritance) inventViewsForInheritance(mainTable)
        else inventViewsForSingleTable(mainTable)
    }

    private fun populateTableWithColumns(table: Table, columnsNumber: Int, exceptColumnNames: MutableSet<String>) {
        val indexColumns: MutableList<Column>? = rnd.nextBoolean ().then { ArrayList<Column>() }
        for (k in 1..columnsNumber) {
            val cName = dict.guessNoun(5, exceptColumnNames, excludedMinorWords)
            val column = TableColumn(table, cName).apply {
                setDataTypeAndDefault(guessSimpleDataType())
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

    private fun inventViewsForSingleTable(table: Table) {
        // Stats
        val discriminantColumn = table.columns.firstOrNull { it.dataType.startsWith("char") || it.dataType == "number(1)" }
        val dataColumns = table.columns
            .filter { it.dataType.startsWith("number(") && it.dataType != "number(1)" && !it.name.endsWith("id") }
        if (discriminantColumn != null && dataColumns.isNotEmpty()) {
            val view = View(*(table.nameWords + "stats"))
            view.baseTables += table
            val keyColumn = discriminantColumn.copyToView(view)
            for (column in dataColumns) {
                val s = column.name
                ViewColumn(view, "min($s) as ${s}_min", *(column.nameWords + "min"))
                ViewColumn(view, "avg($s) as ${s}_avg", *(column.nameWords + "avg"))
                ViewColumn(view, "max($s) as ${s}_max", *(column.nameWords + "max"))
            }
            view.clauseFrom = table.name
            view.clauseGroup = keyColumn.name
            if (!discriminantColumn.mandatory) view.clauseWhere = "${discriminantColumn.name} is not null"
            view.withCheckOption = true
            model.views += view
            usedNames += view.name
            table.associatedViews += view
        }

        // Empties
        val nullableColumns = table.columns.filter { !it.mandatory }
        if (nullableColumns.size >= 2) {
            val view = View(*(table.nameWords + "empties"))
            view.baseTables += table
            for (c in table.columns.filter { it.mandatory }) {
                c.copyToView(view)
            }
            view.clauseFrom = table.name
            view.clauseWhere = nullableColumns.joinToString(separator = "\nand ") { "${it.name} is null" }
            view.withReadOnly = true
            model.views += view
            usedNames += view.name
            table.associatedViews += view
        }
    }


    private fun inventViewsForInheritance(mainTable: Table) {
        for (table in mainTable.inheritedTables) {

            // Whole
            val view = View(*(table.nameWords + "whole"))
            view.baseTables += mainTable
            view.baseTables += table
            val alreadyNames = emptyNameSet()
            for (c in mainTable.columns) {
                c.copyToView(view)
                alreadyNames += c.name
            }
            for (c in table.columns.filter { it.name !in alreadyNames }) {
                c.copyToView(view)
            }
            view.clauseFrom = mainTable.name + " natural join " + table.name
            view.withCheckOption = true
            model.views += view
            usedNames += view.name
            table.associatedViews += view
            
        }
    }


    private fun guessSimpleDataType(): String =
        when (rnd.nextInt(12)) {
            0    -> "char"
            1    -> "char -> '${rnd.nextChar('A','Z')}'"
            2    -> "nchar(${2 + rnd.nextInt(7)})"
            3    -> "varchar(${10 + rnd.nextInt(100) * 10})"
            4    -> "nvarchar2(${40 + rnd.nextInt(10) * 40})"
            5    -> "number(${1 + rnd.nextInt(4)}) -> 0"
            6    -> "number(${1 + rnd.nextInt(5)}) -> -1"
            7    -> "number(${1 + rnd.nextInt(6)})"
            8    -> "number(${1 + rnd.nextInt(7)})"
            9    -> "date"
            10   -> "date -> trunc(sysdate)"
            11   -> "date -> sysdate"
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