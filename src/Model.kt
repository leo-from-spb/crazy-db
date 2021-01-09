package lb.crazydb

import lb.crazydb.TriggerEvent.trigOnInsert
import lb.crazydb.TriggerIncidence.trigBefore
import lb.crazydb.gears.ReservedWords
import lb.crazydb.gears.emptyNameSet
import lb.crazydb.gears.fix
import java.util.concurrent.atomic.AtomicInteger


class Model {

    val order     = ArrayList<MajorObject>()

    val sequences = ArrayList<Sequence>()
    val tables    = ArrayList<Table>()
    val views     = ArrayList<View>()

    val usedNames: MutableSet<String> = emptyNameSet()


    init {
        usedNames.addAll(ReservedWords.words)
    }


    fun newSequence(vararg nameWords: String): Sequence {
        val sequence = Sequence(this, nameWords.fix)
        this.sequences += sequence
        this.order += sequence
        return sequence
    }

    fun newTable(role: TableRole, vararg nameWords: String): Table {
        val table = Table(this, role, nameWords.fix)
        this.tables += table
        this.order += table
        return table
    }

    fun newView(vararg nameWords: String): View {
        val view = View(this, nameWords.fix)
        this.views += view
        this.order += view
        return view
    }

}


private val internalIdSequence = AtomicInteger()


data class NameSpec(val name: String, val spec: String)


////// ABSTRACT HIERARCHY \\\\\\


/**
 * Every object or a detail with it's own name.
 */
sealed class Entity(nameWords: Array<String>) {

    val nameWords: Array<String>
    val name: String

    init {
        assert(nameWords.isNotEmpty())
        this.nameWords = nameWords // TODO remove empty parts
        this.name = nameWords.joinToString(separator = "_")
    }

    override fun toString(): String = name + ':' + this.javaClass.simpleName
    
}


/**
 * Schema object — an object which name must be unique in the schema scope.
 */
sealed class SchemaObject(nameWords: Array<String>) : Entity(nameWords) {

}

/**
 * Independent object (which can be created without other objects),
 * i.e. table, view, routine.
 */
sealed class MajorObject(val model: Model, nameWords: Array<String>) : SchemaObject(nameWords) {

    val intId = internalIdSequence.incrementAndGet()

    var filePrefix: String = ""
    var fileNr = 0

    val fileName: String
        get() = if (fileNr > 0) filePrefix + '_' + fileNr else filePrefix

    fun assignFile(filePrefix: String, fileNr: Int) {
        this.filePrefix = filePrefix
        this.fileNr = fileNr
    }

    fun assignFileFrom(originalObject: MajorObject) {
        this.filePrefix = originalObject.filePrefix
        this.fileNr = originalObject.fileNr
    }

}

/**
 * Minor schema object — a detail of another object but with a name in the schema scope,
 * i.e. index, constraint, trigger.
 */
sealed class MinorObject(val host: MajorObject, nameWords: Array<String>) : SchemaObject(nameWords) {

}

/**
 * Detail, which name is unique in the host's scope, i.e. column.
 */
sealed class InnerObject(val host: MajorObject, nameWords: Array<String>) : Entity(nameWords) {

}



////// OBJECTS \\\\\\


class Sequence (model: Model, nameWords: Array<String>) : MajorObject(model, nameWords) {

    var startWith: Long = 1L

}


enum class TableRole {
    roleAlone,
    roleDictionary,
    roleMain,
    roleCategory,
    roleDetail,
    roleMTM
}


class Table (model: Model, val role: TableRole, nameWords: Array<String>) : MajorObject(model, nameWords) {

    val columns = ArrayList<TableColumn>()
    val indices = ArrayList<Index>()
    val references = ArrayList<Reference>()
    val checks = ArrayList<Check>()
    val triggers = ArrayList<Trigger>()

    var associatedSequence: Sequence? = null
    val associatedViews = ArrayList<View>()

    val inheritedTables = ArrayList<Table>()
    val dependentOnTables = ArrayList<Table>()

    var adjective: String? = null

    val primaryKeySize: Int
        get() = columns.stream().filter(Column::primary).count().toInt()


    fun newColumn(vararg nameWords: String): TableColumn {
        val column = TableColumn(this, nameWords.fix)
        columns += column
        return column
    }

    fun newColumn(nameSpec: NameSpec): TableColumn {
        val column = this.newColumn(nameSpec.name)
        column.setDataTypeAndDefault(nameSpec.spec)
        return column
    }

    fun newIndex(vararg nameWords: String): Index {
        val index = Index(this, nameWords.fix)
        indices += index
        return index
    }

    fun newReference(foreignTable: Table, vararg nameWords: String): Reference {
        val ref = Reference(this, foreignTable, nameWords.fix)
        references += ref
        return ref
    }

    fun newCheck(vararg nameWords: String): Check {
        val check = Check(this, nameWords.fix)
        checks += check
        return check
    }

    fun newTrigger(vararg nameWords: String): Trigger {
        val trigger = Trigger(this, nameWords.fix)
        triggers += trigger
        return trigger
    }

    fun newInheritedTable(nameInhPrefix: String?, nameInhSuffix: String?): Table {
        val inhNameWords = ArrayList<String>()
        inhNameWords.addAll(this.nameWords)
        if (nameInhPrefix != null) {
            val k = 1 // TODO check for the area prefix
            inhNameWords.add(k, nameInhPrefix)
        }
        if (nameInhSuffix != null) {
            inhNameWords += nameInhSuffix
        }
        val inhTableNameWordsArr = inhNameWords.toTypedArray()
        val inhTable = model.newTable(TableRole.roleCategory, *inhTableNameWordsArr)
        inhTable.assignFileFrom(this)
        this.inheritedTables += inhTable

        val basePrimaryColumns: Array<TableColumn> = this.columns.filter(TableColumn::primary).toTypedArray()
        val n = basePrimaryColumns.size
        val inhColumns: Array<TableColumn> = Array(n) { i ->
            val c = basePrimaryColumns[i]
            val inhColumn = c.copyToTable(inhTable)
            inhColumn.mandatory = true
            inhColumn.primary = true
            inhColumn
        }

        val inhReference = inhTable.newReference(this, *(inhTableNameWordsArr + "fk"))
        inhReference.domesticColumns = inhColumns
        inhReference.foreignColumns = basePrimaryColumns
        inhReference.cascade = true

        return inhTable
    }
}


class Check (val table: Table, nameWords: Array<String>) : MinorObject(table, nameWords) {

    var predicate: String = "1 is not null"

}


class View (model: Model, nameWords: Array<String>) : MajorObject(model, nameWords) {
    val columns = ArrayList<ViewColumn>()
    val baseTables = ArrayList<Table>()

    var clauseFrom:  String? = null
    var clauseWhere: String? = null
    var clauseGroup: String? = null

    var withCheckOption = false
    var withReadOnly    = false


    fun newColumn(expression: String, vararg nameWords: String): ViewColumn {
        val column = ViewColumn(this, expression, nameWords.fix)
        columns += column
        return column
    }

}


sealed class Column (host: MajorObject, nameWords: Array<String>) : InnerObject(host, nameWords) {

    var mandatory = false
    var primary = false
    var dataType: String = "char"

    val qName: String
        get() = host.name + '.' + name

    fun copyToView(view: View, qualified: Boolean = false, prefixWord: String? = null): ViewColumn {
        val newNameWords: Array<String> =
            if (prefixWord == null) nameWords
            else arrayOf(prefixWord) + nameWords
        val expression = if (qualified) qName else name
        val newColumn = view.newColumn(expression, *newNameWords)
        newColumn.dataType = dataType
        return newColumn
    }

}


class TableColumn (val table: Table, nameWords: Array<String>) : Column(table, nameWords) {

    var defaultExpression: String? = null

    fun setDataTypeAndDefault(spec: String) {
        val p = spec.indexOf("->")
        if (p < 0) {
            dataType = spec
            defaultExpression = null
        }
        else {
            dataType = spec.substring(0, p).trim()
            defaultExpression = spec.substring(p+2).trim()
        }
    }

    fun copyToTable(table: Table, prefixWord: String? = null): TableColumn {
        val newNameWords: Array<String> =
            if (prefixWord == null) nameWords
            else arrayOf(prefixWord) + nameWords
        val newColumn = table.newColumn(*newNameWords)
        newColumn.dataType = dataType
        return newColumn
    }

}


class ViewColumn (val view: View, expression: String, nameWords: Array<String>) : Column(view, nameWords) {

    var expression: String = expression.trim()

}


class Index (val table: Table, nameWords: Array<String>) : MinorObject(table, nameWords) {

    var unique = false
    var columns: List<Column> = emptyList()

}


class Reference (table: Table, val foreignTable: Table, nameWords: Array<String>) : MinorObject(table, nameWords) {

    var domesticColumns: Array<out Column> = emptyArray()
    var foreignColumns: Array<out Column> = emptyArray()

    var cascade = false

}


enum class TriggerIncidence (val word: String) {
    trigBefore ("before"),
    trigAfter  ("after")
}

enum class TriggerEvent (val word: String) {
    trigOnInsert ("insert"),
    trigOnUpdate ("update"),
    trigOnDelete ("delete")
}


class Trigger (table: Table, nameWords: Array<String>) : MinorObject(table, nameWords) {

    val table: Table get() = host as Table

    var incidence: TriggerIncidence = trigBefore
    var event: TriggerEvent = trigOnInsert
    var forEachRow = false
    var condition: String? = null

    var body: String = "null"

    init {
        table.triggers += this
    }


}



