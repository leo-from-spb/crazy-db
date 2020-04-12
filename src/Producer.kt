package lb.crazydb

import lb.crazydb.gears.*
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.collections.HashSet
import kotlin.streams.toList


class Producer (val model: Model) {

    companion object {
        val scriptsDir = Path.of("scripts")
    }

    val produced: MutableSet<SchemaObject> = HashSet(1000000)

    val producedFiles = TreeSet<Int>()

    private var writer: BufferedWriter? = null


    fun produceWholeScript() {
        val fileNrs = model.order.stream()
            .map { it.fileNr }
            .distinct()
            .sorted()
            .toList()
        for (fileNr in fileNrs) produceFile(fileNr)

        produceCombiningFile()
        printSummary()
    }

    fun produceFile(fileNr: Int) {
        val objects: List<SchemaObject> =
            model.order.stream()
                .filter { it.fileNr == fileNr }
                .filter { it !in produced }
                .toList()
        if (objects.isEmpty()) return

        val tablesFile = scriptsDir.resolve("script_$fileNr.sql")

        this.writer = Files.newBufferedWriter(tablesFile)
        try {
            produceScript(objects)
        }
        finally {
            this.writer!!.close()
            this.writer = null
        }

        producedFiles += fileNr
    }


    fun produceScript(objects: Collection<SchemaObject>) {
        for (obj in objects) produceObject(obj)
    }

    private fun produceObject(o: SchemaObject) {
        when (o) {
            is Sequence -> produceSequence(o)
            is Table    -> produceTable(o)
            is View     -> produceView(o)
            is Trigger  -> produceTrigger(o)
        }
    }

    private fun produceSequence(sequence: Sequence) {
        if (sequence in produced) return

        val b = StringBuilder(1024)

        b.phrase("create sequence", sequence.name).eoln()
        if (sequence.startWith != 1L) b.tab().phrase("start with", sequence.startWith.toString()).eoln()
        b.append("/\n\n")

        write(b)
        produced += sequence
    }

    private fun produceTable(table: Table) {
        if (table in produced) return

        val b = StringBuilder(1024)

        val sequence = table.associatedSequence
        if (sequence != null) produceSequence(sequence)

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
            produced += i
        }
        for (r in table.references) {
            val columns = r.domesticColumns.parenthesized { it.name }
            val cascade = r.cascade.then("on delete cascade")
            b.tab().phrase("constraint", r.name, "foreign key", columns, "references", r.foreignTable.name, cascade).comma().eoln()
            produced += r
        }
        for (ch in table.checks) {
            b.tab().phrase("constraint", ch.name, "check", '('+ch.predicate+')').comma().eoln()
            produced += ch
        }
        b.removeEnding(',')
        b.append(")\n/\n\n")

        write(b)
        produced += table

        for (index in table.indices) produceIndex(index)
        for (trigger in table.triggers) produceTrigger(trigger)
        for (view in table.associatedViews) produceView(view)
    }


    private fun produceView(view: View) {
        if (view in produced) return

        val b = StringBuilder()
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

        write(b)
        produced += view
    }


    private fun produceIndex(index: Index) {
        if (index in produced) return

        val table = index.table
        val uq = index.unique.then("unique")

        val b = StringBuilder()

        b.phrase("create", uq, "index", index.name, "on", table.name, index.columns.parenthesized { it.name }).eoln()
        b.append("/\n\n")

        write(b)
        produced += index
    }


    private fun produceTrigger(trigger: Trigger) {
        if (trigger in produced) return

        val b = StringBuilder()

        b.phrase("create trigger", trigger.name).eoln()
        b.tab().phrase(trigger.incidence.word, trigger.event.word, "on", trigger.table.name).eoln()
        if (trigger.forEachRow) b.tab().append("for each row").eoln()
        if (trigger.condition != null) b.tab().append("when (",trigger.condition,")\n")
        b.append("begin\n")
        b.append(trigger.body shiftTextWith '\t').eolnIfNo()
        b.append("end;\n/\n\n")

        write(b)
        produced += trigger
    }


    private fun produceCombiningFile() {
        val combineFile = scriptsDir.resolve("script.sql")
        this.writer = Files.newBufferedWriter(combineFile)
        try {
            produceCombiningFileContent()
        }
        finally {
            this.writer!!.close()
            this.writer = null
        }
    }

    private fun produceCombiningFileContent() {
        val b = StringBuilder()
        b.append("-- CRAZY DB --\n\n")

        for (fileNr in producedFiles) {
            b.append("@@script_").append(fileNr).eoln()
        }

        b.append("\n-- OK --\n")
        write(b)
    }


    private fun write(text: CharSequence) {
        val writer = this.writer ?: throw IllegalStateException("Writer is not initialized")
        writer.write(text.toString())
        if (!text.endsWith('\n')) writer.write("\n")
    }


    private fun printSummary() {
        var sequences = 0
        var tables    = 0
        var views     = 0
        var indices   = 0
        var triggers  = 0
        var fks       = 0
        var checks    = 0

        for (p in produced)
            when (p) {
                is Sequence  -> sequences++
                is Table     -> tables++
                is View      -> views++
                is Index     -> indices++
                is Trigger   -> triggers++
                is Reference -> fks++
                is Check     -> checks++
            }

        val message = """|Generated:
                         |~${producedFiles.size}~files
                         |~$sequences~sequences
                         |~$tables~tables
                         |~$views~views
                         |~$indices~indices
                         |~$triggers~triggers
                         |~$fks~foreign keys
                         |~$checks~checks
                      """.trimMargin().replace('~','\t')
        say(message)
    }

}