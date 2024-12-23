package lb.crazy.dedal.generator

import lb.crazy.dedal.dict.WordBook
import lb.crazy.model.*

/**
 * Generates a simple schema.
 */
class BasicSchemaGenerator : AbstractSchemaGenerator {

    val book: WordBook

    constructor(model: Model, schemaName: String, book: WordBook) : super(model, schemaName) {
        this.book = book
    }


    override fun generate() {
        for (i in 1..3) {
            val area = schema.newArea("D$i")
            val generator = SubjectAreaGenerator(area)
            generator.makePrimeTables()
        }
    }


    private inner class SubjectAreaGenerator (val area: SubjectArea) {

        fun makePrimeTables() {
            for (i in 1..20) {
                val oldTablesN = schema.tables.size
                val newTable =
                    makeOnePrimeTable()
                if (oldTablesN > 0 && rnd.nextInt(4) == 0) {
                    val oldTable = schema.tables[rnd.nextInt(oldTablesN)]
                    makeManyToManyTable(oldTable, newTable)
                }
            }
        }


        private fun makeOnePrimeTable(): Table {
            val rootWord = book.nouns.guessWord(5, schema.innerNames)
            val table = area.newTable(rootWord, TableRole.Master)

            makeIdColumn(table)
            for (i in 1..rnd.nextInt(1, 15)) makeSimpleColumn(table)

            return table
        }

        private fun makeIdColumn(table: Table): Column {
            val type: Type =
                when (rnd.nextInt(5)) {
                    1 -> IntType.int1
                    2 -> IntType.int2
                    3 -> IntType.int4
                    4 -> IntType.int8
                    else -> DecimalIntType(4 + rnd.nextInt(15))
                }
            val column = table.newColumn("Id", type, mandatory = true)
            table.newIndex(null, "Id", primary = true)
            return column
        }

        private fun makeManyToManyTable(table1: Table, table2: Table) {
            assert(table1 !== table2)
            assert(table1.wordRoot !== table2.wordRoot)
            val refKey1 = table1.primaryKey ?: return
            val refKey2 = table2.primaryKey ?: return

            val combinedRoot = table1.wordRoot + '_' + table2.wordRoot
            val table = area.newTable(combinedRoot, TableRole.ManyToMany)
            val fk1 = table.newForeignKeyAndColumns(refKey1, table1.wordRoot, mandatory = true)
            val fk2 = table.newForeignKeyAndColumns(refKey2, table2.wordRoot, mandatory = true)

            val pkColumnNames = fk1.domColumns.toTypedArray() + fk2.domColumns.toTypedArray()
            val withExtraColumns = rnd.nextInt(3) == 0
            table.newIndex(null, *pkColumnNames, unique = true, primary = withExtraColumns)

            if (withExtraColumns) {
                makeSimpleColumn(table)
                if (rnd.nextBoolean())
                    makeSimpleColumn(table)
            }
        }

        private fun makeSimpleColumn(table: Table) {
            val noun = book.nouns.guessWord(5, table.innerNames)
            val adjective = book.adjectives.guessWord(2)
            val name = adjective + '_' + noun
            val type =
                when (rnd.nextInt(6)) {
                    0 -> BoolType
                    1 -> IntType.int2
                    2 -> IntType.int4
                    3 -> IntType.int8
                    4 -> DecimalIntType(rnd.nextInt(1, 30))
                    else -> StrType(rnd.nextInt(8, 40))
                }
            table.newColumn(name, type)
        }

    }

}