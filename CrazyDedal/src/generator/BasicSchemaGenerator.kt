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
            val type = NumType(rnd.nextBoolean().choose(sizeNorm, sizeLong), true)
            val column = table.newColumn("Id", type, mandatory = true)
            table.newIndex(null, "Id", primary = true)
            return column
        }

        private fun makeManyToManyTable(table1: Table, table2: Table) {
            assert(table1 !== table2)
            assert(table1.wordRoot !== table2.wordRoot)
            val combinedRoot = table1.wordRoot + '_' + table2.wordRoot
            val table = area.newTable(combinedRoot, TableRole.ManyToMany)
            val indexColumnNames = ArrayList<String>(2)
            for (c1 in table1.primaryColumns) {
                val name = table1.wordRoot + '_' + c1.name
                table.newColumn(name, c1.type, mandatory = true)
                indexColumnNames += name
            }
            for (c2 in table2.primaryColumns) {
                val name = table2.wordRoot + '_' + c2.name
                table.newColumn(name, c2.type, mandatory = true)
                indexColumnNames += name
            }
            if (rnd.nextBoolean()) {
                makeSimpleColumn(table)
                table.newIndex(null, *indexColumnNames.toTypedArray(), primary = true)
            }
            else {
                table.newIndex(null, *indexColumnNames.toTypedArray(), unique = true)
            }
        }

        private fun makeSimpleColumn(table: Table) {
            val noun = book.nouns.guessWord(5, table.innerNames)
            val adjective = book.adjectives.guessWord(2)
            val name = adjective + '_' + noun
            val type = NumType(rnd.nextInt(1, 18))
            table.newColumn(name, type)
        }

    }

}