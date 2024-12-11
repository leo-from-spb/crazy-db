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
        makePrimeTables()
    }


    private fun makePrimeTables() {
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
        val tableName = book.nouns.guessWord(5, schema.innerNames)
        val table = Table(TableRole.Master, tableName)

        makeIdColumn(table)
        for (i in 1..rnd.nextInt(1,15)) makeSimpleColumn(table)

        schema addTable table
        return table
    }

    private fun makeIdColumn(table: Table): Column {
        val type = NumType(rnd.nextBoolean().choose(sizeNorm, sizeLong), true)
        val column = Column("Id", type, mandatory = true)
        table addColumn column
        val key = Index(table.name + "_pk", "Id", primary = true)
        table addIndex key
        return column
    }

    private fun makeManyToManyTable(table1: Table, table2: Table) {
        assert(table1 !== table2)
        assert(table1.name !== table2.name)
        val tableName = table1.name + '_' + table2.name
        val table = Table(TableRole.ManyToMany, tableName)
        val indexColumnNames = ArrayList<String>(2)
        for (c1 in table1.primaryColumns) {
            val name = table1.name + '_' + c1.name
            table addColumn Column(name, c1.type, mandatory = true)
            indexColumnNames += name
        }
        for (c2 in table2.primaryColumns) {
            val name = table2.name + '_' + c2.name
            table addColumn Column(name, c2.type, mandatory = true)
            indexColumnNames += name
        }
        if (rnd.nextBoolean()) {
            makeSimpleColumn(table)
            table addIndex Index(tableName + "_pk", *indexColumnNames.toTypedArray(), primary = true)
        }
        else {
            table addIndex Index(tableName + "_ui", *indexColumnNames.toTypedArray(), unique = true)
        }
        schema addTable table
    }

    private fun makeSimpleColumn(table: Table) {
        val noun = book.nouns.guessWord(5, table.innerNames)
        val adjective = book.adjectives.guessWord(2)
        val name = adjective + '_' + noun
        val type = NumType(rnd.nextInt(1, 18))
        val column = Column(name, type)
        table addColumn column
    }

}