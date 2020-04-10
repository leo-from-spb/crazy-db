package lb.crazydb

import lb.crazydb.TriggerEvent.trigOnInsert
import lb.crazydb.TriggerIncidence.trigBefore
import lb.crazydb.gears.ReservedWords
import lb.crazydb.gears.emptyNameSet


class Model {

    val sequences = ArrayList<Sequence>()
    val tables    = ArrayList<Table>()
    val views     = ArrayList<View>()

    val usedNames: MutableSet<String> = emptyNameSet()


    init {
        usedNames.addAll(ReservedWords.words)
    }

}



sealed class Entity(vararg nameWords: String) {

    val nameWords: Array<out String>
    val name: String

    init {
        assert(nameWords.isNotEmpty())
        this.nameWords = nameWords
        this.name = nameWords.joinToString(separator = "_")
    }

    override fun toString(): String = name + ':' + this.javaClass.simpleName
    
}


class Sequence (vararg nameWords: String) : Entity(*nameWords) {

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


class Table (val role: TableRole, vararg nameWords: String) : Entity(*nameWords) {

    val columns = ArrayList<TableColumn>()
    val indices = ArrayList<Index>()
    val references = ArrayList<Reference>()
    val checks = ArrayList<Check>()
    val triggers = ArrayList<Trigger>()

    var associatedSequence: Sequence? = null
    val associatedViews = ArrayList<View>()

    val inheritedTables = ArrayList<Table>()
    val dependentOnTables = ArrayList<Table>()

    val primaryKeySize: Int
        get() = columns.stream().filter(Column::primary).count().toInt()

}


class Check (val table: Table, vararg nameWords: String) : Entity(*nameWords) {

    var predicate: String = "1 is not null"

    init {
        table.checks += this
    }

}


class View (vararg nameWords: String) : Entity(*nameWords) {
    val columns = ArrayList<ViewColumn>()
    val baseTables = ArrayList<Table>()

    var clauseFrom:  String? = null
    var clauseWhere: String? = null
    var clauseGroup: String? = null

    var withCheckOption = false
    var withReadOnly    = false
}


sealed class Column (val host: Entity, vararg nameWords: String) : Entity(*nameWords) {

    var mandatory = false
    var primary = false
    var dataType: String = "char"

    val qName: String
        get() = host.name + '.' + name

    fun copyToView(view: View, qualified: Boolean = false, prefixWord: String? = null): ViewColumn {
        val newNameWords: Array<out String> =
            if (prefixWord == null) nameWords
            else arrayOf(prefixWord) + nameWords
        val expression = if (qualified) qName else name
        val newColumn = ViewColumn(view, expression, *newNameWords)
        newColumn.dataType = dataType
        return newColumn
    }

}


class TableColumn (val table: Table, vararg nameWords: String) : Column(table, *nameWords) {

    var defaultExpression: String? = null

    init {
        table.columns += this
    }

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
        val newNameWords: Array<out String> =
            if (prefixWord == null) nameWords
            else arrayOf(prefixWord) + nameWords
        val newColumn = TableColumn(table, *newNameWords)
        newColumn.dataType = dataType
        return newColumn
    }

}


class ViewColumn (val view: View, expression: String, vararg nameWords: String) : Column(view, *nameWords) {

    var expression: String = expression.trim()

    init {
        view.columns += this
    }

}


class Index (val table: Table, vararg nameWords: String) : Entity(*nameWords) {

    var unique = false
    var columns: List<Column> = emptyList()

    init {
        table.indices += this
    }

}


class Reference (val table: Table, val foreignTable: Table, vararg nameWords: String) : Entity(*nameWords) {

    var domesticColumns: Array<Column> = emptyArray()
    var foreignColumns: Array<Column> = emptyArray()

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


class Trigger (val table: Table, vararg nameWords: String) : Entity(*nameWords) {

    var incidence: TriggerIncidence = trigBefore
    var event: TriggerEvent = trigOnInsert
    var forEachRow = false
    var condition: String? = null

    var body: String = "null"

    init {
        table.triggers += this
    }


}



