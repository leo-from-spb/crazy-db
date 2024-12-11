package lb.crazy.model

import java.util.*


class Model {

    val schemas = ArrayList<Schema>()
    


}


sealed class NamedEntity (val name: String?) {

    val innerNames = HashSet<String>()

    init {
        assert(name == null || name.isNotEmpty())
    }

    protected fun registerInnerElement(element: NamedEntity) {
        val innerName = element.name
        if (innerName != null) innerNames += innerName
    }

}


class Schema : NamedEntity {

    val tables = ArrayList<Table>()

    constructor(name: String) : super(name)

    infix fun addTable(table: Table) {
        registerInnerElement(table)
        tables += table
    }

}


class Table : NamedEntity {

    val role: TableRole

    val columns = ArrayList<Column>()
    val indices = ArrayList<Index>()

    constructor(role: TableRole, name: String) : super(name) {
        this.role = role
    }

    infix fun addColumn(column: Column) {
        registerInnerElement(column)
        columns += column
    }

    infix fun addIndex(index: Index) {
        if (index.primary) assert(indices.none { it.primary })
        for (columnName in index.columnNames) assert(columns.any { it.name == columnName })
        registerInnerElement(index)
        indices += index
    }

    val primaryColumns: List<Column>
        get() {
            val pk = indices.find { it.primary } ?: return emptyList()
            return pk.columnNames.map { columnName -> columns.find { it.name == columnName }!! }
        }
}


class Column : NamedEntity {

    var primaRef: Column? = null
    var ownType: Type? = null
    var mandatory: Boolean = false

    constructor(name: String, primaRef: Column, mandatory: Boolean = false) : super(name) {
        this.primaRef = primaRef
        this.mandatory = mandatory
    }

    constructor(name: String, type: Type, mandatory: Boolean = false) : super(name) {
        this.ownType = type
        this.mandatory = mandatory
    }


    val type: Type = primaRef?.type ?: ownType ?: BoolType

}


class Index : NamedEntity {

    val columnNames: List<String>

    val unique: Boolean
    val primary: Boolean

    constructor(name: String?, vararg columnNames: String, unique: Boolean = false, primary: Boolean = false) : super(name) {
        this.columnNames =
            when (columnNames.size) {
                0    -> throw IllegalArgumentException("Columns are missing in the index $name")
                1    -> Collections.singletonList<String>(columnNames[0])
                else -> columnNames.asList()
            }
        this.unique = unique || primary
        this.primary = primary
    }
    
}

