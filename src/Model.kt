package lb.crazydb

import lb.crazydb.TriggerEvent.trigOnInsert
import lb.crazydb.TriggerIncidence.trigBefore
import lb.crazydb.gears.ReservedWords
import lb.crazydb.gears.emptyNameSet


class Model {

    val sequences = ArrayList<Sequence>()
    val tables    = ArrayList<Table>()

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

    val columns = ArrayList<Column>()
    val indices = ArrayList<Index>()
    val references = ArrayList<Reference>()
    val checks = ArrayList<Check>()
    val triggers = ArrayList<Trigger>()

    var associatedSequence: Sequence? = null
    
    val dependentOnTables: Set<Table> = emptySet()

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
    val columns = ArrayList<Column>()
    val baseTables = ArrayList<Table>()
}


class Column (val table: Table, vararg nameWords: String) : Entity(*nameWords) {

    var mandatory = false
    var primary = false
    var dataType: String = "char"

    init {
        table.columns += this
    }

    fun copyTo(table: Table, prefixWord: String? = null): Column {
        val newNameWords: Array<out String> =
            if (prefixWord == null) nameWords
            else arrayOf(prefixWord) + nameWords
        val newColumn = Column(table, *newNameWords)
        newColumn.dataType = dataType
        return newColumn
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



