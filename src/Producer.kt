package lb.crazydb

import lb.crazydb.gears.*
import java.io.BufferedWriter
import java.lang.StringBuilder
import java.nio.file.Files
import java.nio.file.Path


class Producer (val model: Model) {

    val generatedTables: MutableSet<Table> = HashSet()


    fun produceAll() {
        val tablesFile = Path.of("tables.sql")
        Files.newBufferedWriter(tablesFile).use { writer ->
            produceTables(model.tables, writer)
        }
        say("Produced ${generatedTables.size} tables.")
    }


    fun produceTables(tables: Collection<Table>, writer: BufferedWriter) {
        for (table in tables) {
            if (table in generatedTables) continue
            val text: String = generateTable(table)
            assert(text.endsWith('\n'))
            writer.write(text)
            writer.write("/\n\n")
            generatedTables += table
        }
    }

    private fun generateTable(table: Table): String {
        val pks = table.primaryKeySize
        val b = StringBuilder(1024)
        b.append("create table ").append(table.name).append('\n')
        b.append("(\n")
        for (column in table.columns) {
            b.tab().phrase(column.name, column.dataType, column.mandatory.then("not null"), (column.primary && pks == 1).then("primary key")).comma().eoln()
        }
        if (pks >= 2) {
            val ct = table.columns.filter(Column::primary).joinToString { it.name }
            b.tab().append("primary key (").append(ct).append("),\n")
        }
        b.removeEnding(',')
        b.append(")\n")
        return b.toString()
    }

}