package lb.crazydb

import lb.crazydb.gears.*
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path


class Producer (val model: Model) {

    val generatedSequences: MutableSet<Sequence> = HashSet()
    val generatedTables:    MutableSet<Table>    = HashSet()
    val generatedViews:     MutableSet<View>     = HashSet()


    fun produceAll() {
        val tablesFile = Path.of("tables.sql")
        Files.newBufferedWriter(tablesFile).use { writer ->
            produceTables(model.tables, writer)
        }
        say("Produced: \n" +
            "\t${generatedSequences.size}\tsequences,\n" +
            "\t${generatedTables.size}\ttables,\n" +
            "\t${generatedViews.size}\tviews.\n")
    }


    fun produceTables(tables: Collection<Table>, writer: BufferedWriter) {
        for (table in tables) {
            if (table in generatedTables) continue
            val text: String = generateTable(table)
            assert(text.endsWith('\n'))
            writer.write(text)
            generatedTables += table
        }
    }

    private fun generateTable(table: Table): String {
        val b = StringBuilder(1024)

        val sequence = table.associatedSequence
        if (sequence != null && sequence !in generatedSequences) {
            b.phrase("create sequence", sequence.name).eoln()
            if (sequence.startWith != 1L) b.tab().phrase("start with", sequence.startWith.toString()).eoln()
            b.append("/\n\n")
            generatedSequences += sequence
        }

        val pks = table.primaryKeySize
        b.append("create table ").append(table.name).append('\n')
        b.append("(\n")
        for (column in table.columns) {
            val default = if (column.defaultExpression != null) "default (${column.defaultExpression})" else null
            val pk = (column.primary && pks == 1).then("primary key")
            b.tab().phrase(column.name, column.dataType, default, column.mandatory.then("not null"), pk).comma().eoln()
        }
        if (pks >= 2) {
            val ct = table.columns.filter(Column::primary).joinToString { it.name }
            b.tab().append("primary key (").append(ct).append("),\n")
        }
        for (i in table.indices.filter(Index::unique)) {
            b.tab().phrase("constraint", i.name, "unique", i.columns.parenthesized { it.name }).comma().eoln()
        }
        for (r in table.references) {
            val columns = r.domesticColumns.parenthesized { it.name }
            val cascade = r.cascade.then("on delete cascade")
            b.tab().phrase("constraint", r.name, "foreign key", columns, "references", r.foreignTable.name, cascade).comma().eoln()
        }
        for (ch in table.checks) {
            b.tab().phrase("constraint", ch.name, "check", '('+ch.predicate+')').comma().eoln()
        }
        b.removeEnding(',')
        b.append(")\n/\n\n")

        for (i in table.indices.filter { !it.unique }) {
            b.phrase("create index", i.name, "on", table.name, i.columns.parenthesized { it.name }).eoln()
            b.append("/\n\n")
        }

        for (trigger in table.triggers) {
            b.phrase("create trigger", trigger.name).eoln()
            b.tab().phrase(trigger.incidence.word, trigger.event.word, "on", table.name).eoln()
            if (trigger.forEachRow) b.tab().append("for each row").eoln()
            if (trigger.condition != null) b.tab().append("when (",trigger.condition,")\n")
            b.append("begin\n")
            b.append(trigger.body shiftTextWith '\t').eolnIfNo()
            b.append("end;\n/\n\n")
        }

        for (view in table.associatedViews) {
            if (view in generatedViews) continue
            b.phrase("create view", view.name, "as").eoln()
            var first = true
            for (c in view.columns) {
                val begin = if (first) "select " else "       "
                b.append(begin).phrase(c.expression).comma().eoln()
                first = false
            }
            b.removeEnding()
            if (view.clauseFrom != null) b.phrase("from", view.clauseFrom!! shiftTextBodyWith "     ").eoln()
            if (view.clauseWhere != null) b.phrase("where", view.clauseWhere!! shiftTextBodyWith "  ").eoln()
            if (view.clauseGroup != null) b.phrase("group by", view.clauseGroup!! shiftTextBodyWith "      ").eoln()
            if (view.withCheckOption) b.append("with check option").eoln()
            if (view.withReadOnly) b.append("with read only").eoln()
            b.append("/\n\n")
            generatedViews += view
        }

        return b.toString()
    }

}