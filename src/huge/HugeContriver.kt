package lb.crazydb.huge

import lb.crazydb.Column
import lb.crazydb.Model
import lb.crazydb.Table
import lb.crazydb.TableRole.roleMain
import lb.crazydb.TriggerEvent.trigOnInsert
import lb.crazydb.TriggerIncidence.trigBefore
import lb.crazydb.gears.*
import lb.crazydb.gears.Dictionary
import java.util.*
import kotlin.collections.ArrayList


class HugeContriver(val model: Model, val dict: Dictionary, val areaPrefix: String) {

    private val usedNames get() = model.usedNames


    private val specialWords: Set<String> = wordSetOf("ID", "NR", "ORDER_NR")

    private val excludedMinorWords: Set<String>

    private val rnd = Random(System.nanoTime() * 17L + areaPrefix.hashCode())


    init {
        excludedMinorWords = emptyNameSet()
        excludedMinorWords += specialWords
        excludedMinorWords += ReservedWords.words

        usedNames += specialWords
    }

    fun inventCrazySchema(numberOfFiles: Int, numberOfPortionsPerFile: Int) {
        for (fileNr in 1 .. numberOfFiles) inventFile(fileNr, numberOfPortionsPerFile)
    }

    fun inventFile(fileNr: Int, numberOfPortions: Int) {
        for (i in 1 .. numberOfPortions) inventPortion(fileNr, i)
    }

    private fun inventPortion(fileNr: Int, portionIndex: Int) {
        val mainWord = dict.guessNoun(6, usedNames)
        val mainAbb = mainWord.abb(4)
        if (mainAbb.length < 2 || mainAbb in usedNames) return

        val mainSequence = model.newSequence(areaPrefix, mainWord, "seq")
        mainSequence.assignFile(areaPrefix, fileNr)
        mainSequence.startWith = portionIndex.toLong()
        usedNames += mainSequence.name

        val inheritance = rnd.nextBoolean()
        val inheritedTableNumber = if (inheritance) 2 + rnd.nextInt(6) else 0

        val mainTable = model.newTable(roleMain, areaPrefix, mainWord)
        mainTable.assignFile(areaPrefix, fileNr)
        mainTable.associatedSequence = mainSequence

        val exceptColumnNames = newNameSet(mainAbb)

        val keyColumn = mainTable.newColumn("id").apply {
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
            mainTable.newColumn(name).apply {
                dataType = when (rnd.nextInt(3)) {
                    1    -> "char(1)"
                    2    -> "char(2)"
                    else -> "number(1)"
                }
                mandatory = rnd.nextBoolean() || rnd.nextBoolean()
            }
            exceptColumnNames += name
        }

        val keyCheck = mainTable.newCheck(areaPrefix, mainWord, keyColumn.name, "ch").apply {
            predicate = keyColumn.name + " > 0"
        }

        val columnsNumber =
            if (inheritance) 1 + rnd.nextInt(7)
            else 3 + rnd.nextInt(30)
        populateTableWithColumns(mainTable, columnsNumber, exceptColumnNames)

        val trigger = mainTable.newTrigger(*(mainTable.nameWords + "id" + "trg")).apply {
            incidence = trigBefore
            event = trigOnInsert
            forEachRow = true
            condition = "new.id is null"
            body = """|select ${mainSequence.name}.nextval
                      |into :new.${keyColumn.name}
                      |from dual;
                   """.trimMargin()
        }

        usedNames += mainWord
        usedNames += mainAbb
        usedNames += keyCheck.name
        usedNames += trigger.name

        for (k in 1..inheritedTableNumber) {
            val adjective = dict.guessAdjective(3, usedNames, exceptColumnNames)
            val inhTable = mainTable.newInheritedTable(adjective, null)
            inhTable.adjective = adjective
            populateTableWithColumns(inhTable, 1 + rnd.nextInt(26), exceptColumnNames)
            usedNames += inhTable.name
        }

        if (inheritance) inventViewsForInheritance(mainTable)
        else inventViewsForSingleTable(mainTable)
    }

    private fun populateTableWithColumns(table: Table, columnsNumber: Int, exceptColumnNames: MutableSet<String>) {
        val indexColumns: MutableList<Column>? = rnd.nextBoolean ().then { ArrayList<Column>() }
        for (k in 1..columnsNumber) {
            val cName = dict.guessNoun(5, exceptColumnNames, excludedMinorWords)
            val dt = guessSimpleDataType()
            val column = table.newColumn(cName).apply {
                setDataTypeAndDefault(dt)
                mandatory = "default" in dataType || rnd.nextBoolean() && rnd.nextBoolean()
            }
            exceptColumnNames += cName
            if (indexColumns != null && rnd.nextInt(2) == 0 && "number" in column.dataType) {
                indexColumns.add(column)
            }
            if (rnd.nextInt(3) == 0 && "number" in dt) {
                val digit = 2 + rnd.nextInt(8)
                val r: IntRange? =
                    when (dt) {
                        "number(2)"  -> 1..digit*10
                        "number(3)"  -> 1..digit*100
                        "number(4)"  -> 1..digit*1000
                        "number(5)"  -> 1..digit*10000
                        "number(6)"  -> 1..digit*100000
                        "number(7)"  -> 1..digit*1000000
                        "number(8)"  -> 1..digit*10000000
                        "number(9)"  -> 1..digit*100000000
                        "number(10)" -> 1..2000000000
                        else         -> null
                    }
                if (r != null)
                    table.newCheck(*(table.nameWords + cName + "ch")).apply {
                        predicate = "$cName between ${r.first} and ${r.last}"
                    }
            }
        }
        if (indexColumns != null && indexColumns.isNotEmpty()) {
            val indexNameWords =
                if (indexColumns.size == 1) table.nameWords + indexColumns.first().name + "i"
                else table.nameWords + "x" + "i"
            table.newIndex(*indexNameWords).apply {
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
            val view = model.newView(*(table.nameWords + "stats"))
            view.baseTables += table
            view.assignFileFrom(table)
            val keyColumn = discriminantColumn.copyToView(view)
            for (column in dataColumns) {
                val s = column.name
                view.newColumn("min($s) as ${s}_min", *(column.nameWords + "min"))
                view.newColumn("avg($s) as ${s}_avg", *(column.nameWords + "avg"))
                view.newColumn("max($s) as ${s}_max", *(column.nameWords + "max"))
            }
            view.clauseFrom = table.name
            view.clauseGroup = keyColumn.name
            if (!discriminantColumn.mandatory) view.clauseWhere = "${discriminantColumn.name} is not null"
            usedNames += view.name
            table.associatedViews += view
        }

        // Empties
        val nullableColumns = table.columns.filter { !it.mandatory }
        if (nullableColumns.size >= 2) {
            val partial = rnd.nextBoolean()
            val suffix = if (partial) "partial_empty" else "whole_empty"
            val joinOperator = if (partial) "or" else "and"
            val view = model.newView(*(table.nameWords + suffix))
            view.baseTables += table
            view.assignFileFrom(table)
            for (c in table.columns.filter { it.mandatory }) {
                c.copyToView(view)
            }
            view.clauseFrom = table.name
            view.clauseWhere = nullableColumns.joinToString(separator = "\n$joinOperator ") { "${it.name} is null" }
            if (partial) view.withCheckOption = true else view.withReadOnly = true
            usedNames += view.name
            table.associatedViews += view
        }
    }


    private fun inventViewsForInheritance(mainTable: Table) {
        // Whole
        for (table in mainTable.inheritedTables) {
            val view = model.newView(*(table.nameWords + "whole"))
            view.baseTables += mainTable
            view.baseTables += table
            view.assignFileFrom(table)
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
            usedNames += view.name
            table.associatedViews += view
        }

        // Coupled views
        val inhTables: List<Table> = mainTable.inheritedTables.filter { it.adjective != null }
        if (inhTables.size >= 3 && mainTable.nameWords.size == 1) {
            for (tab1 in inhTables) for (tab2 in inhTables) {
                if (tab1 === tab2) continue
                if (!rnd.nextBoolean()) continue
                val view = model.newView(areaPrefix, tab1.adjective!!, tab2.adjective!!, mainTable.name)
                view.baseTables += mainTable
                view.baseTables += tab1
                view.baseTables += tab2
                val laterTab = if (tab1.intId < tab2.intId) tab2 else tab1
                view.assignFileFrom(laterTab)
                for (c in mainTable.columns) c.copyToView(view, true)
                for (c in tab1.columns) if (!c.primary) c.copyToView(view, true)
                for (c in tab2.columns) if (!c.primary) c.copyToView(view, true)
                view.clauseFrom = """|${mainTable.name}
                                     |left join ${tab1.name} on ${mainTable.name}.id = ${tab1.name}.id
                                     |left join ${tab2.name} on ${mainTable.name}.id = ${tab2.name}.id
                                  """.trimMargin()
                view.withReadOnly = true
                usedNames += view.name
            }
        }
    }


    private fun guessSimpleDataType(): String =
        when (rnd.nextInt(13)) {
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
            12   -> "raw(${4 + rnd.nextInt(16)*4})"
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