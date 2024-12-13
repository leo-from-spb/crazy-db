package lb.crazy.model

import java.util.*


class Model {

    val schemas = ArrayList<Schema>()

    val schemasByName: MutableMap<String, Schema> = HashMap()

    fun newSchema(name: String): Schema {
        assert(name !in schemasByName.keys)
        val schema = Schema(name)
        schemas += schema
        schemasByName[name] = schema
        return schema
    }

    fun obtainSchema(name: String): Schema =
        schemasByName[name]
        ?:
        newSchema(name)
    
}


sealed class Entity {

    abstract val parent: Entity?

}


sealed class NamedEntity (val name: String?): Entity() {

    val innerNames = HashSet<String>()

    init {
        assert(name == null || name.isNotEmpty())
    }

    protected open fun registerInnerElement(element: NamedEntity) {
        val innerName = element.name
        if (innerName != null) innerNames += innerName
    }

    override fun toString() = name ?: "<unnamed>"
    
}


class Schema : NamedEntity {

    override val parent: Entity?
        get() = null

    val areas: ArrayList<SubjectArea> = ArrayList()
    val tables = ArrayList<Table>()

    val areasByName: MutableMap<String, SubjectArea> = LinkedHashMap()

    val boringArea: SubjectArea

    constructor(name: String) : super(name) {
        boringArea = SubjectArea(this, null)
    }

    fun newArea(prefix: String): SubjectArea {
        assert(prefix !in areasByName.keys)
        val area = SubjectArea(this, prefix)
        areas += area
        areasByName[prefix] = area
        return area
    }

    override fun toString(): String = "$name (${areas.size} areas, ${tables.size} tables)"

}


class SubjectArea (val schema: Schema, val prefix: String?): Entity() {

    override val parent: Entity
        get() = schema

    val rootWords: MutableMap<String, MajorObject> = HashMap()

    val tables = ArrayList<Table>()

    fun newTable(rootWord: String, role: TableRole): Table {
        assert (rootWord !in rootWords.keys)
        val table = Table(this, rootWord, role)
        tables += table
        schema.tables += table
        return table
    }

    override fun toString(): String = "$prefix (${tables.size} tables)"

}


sealed class MajorObject : NamedEntity {

    val area: SubjectArea?
    val wordRoot: String?

    override val parent: Entity?
        get() = area

    constructor(area: SubjectArea, wordRoot: String?) : super(nameOf(area, wordRoot)) {
        this.area = area
        this.wordRoot = wordRoot
    }
}


sealed class MinorElement : NamedEntity {

    val major: MajorObject

    override val parent: Entity?
        get() = major

    constructor(major: MajorObject, name: String?) : super(name) {
        this.major = major
    }

}


private fun nameOf(area: SubjectArea, rootWord: String?): String? =
    when {
        rootWord == null -> null
        area.prefix == null -> rootWord
        else -> area.prefix + '_' + rootWord
    }


class Table : MajorObject {

    val role: TableRole

    val columns = ArrayList<Column>()
    val indices = ArrayList<Index>()
    val foreignKeys = ArrayList<ForeignKey>()

    val columnsByName = HashMap<String, Column>()

    var primaryKey: Index? = null
        private set

    private var indexCounter = 0

    constructor(area: SubjectArea, rootWord: String, role: TableRole) : super(area, rootWord) {
        this.role = role
    }

    fun newColumn(columnPrefix: String?, primaRef: Column, mandatory: Boolean = false): Column {
        val name = primaRef.name?.withPrefix(columnPrefix, '_') ?: "column_${columns.size + 1}"
        assert (name !in columnsByName.keys)
        val column = Column(this, name, primaRef, mandatory)
        registerColumn(column)
        return column
    }

    fun newColumn(name: String, type: Type, mandatory: Boolean = false): Column {
        assert (name !in columnsByName.keys)
        val column = Column(this, name, type, mandatory)
        registerColumn(column)
        return column
    }

    private fun registerColumn(column: Column) {
        registerInnerElement(column)
        columns += column
        columnsByName[column.name!!] = column
    }

    fun newIndex(name: String?, vararg columnNames: String, unique: Boolean = false, primary: Boolean = false): Index {
        val indexColumns = this.columns.byNames(*columnNames)
        return newIndex(name, *indexColumns.toTypedArray(), unique = unique, primary = primary)
    }

    fun newIndex(name: String?, vararg indexColumns: Column, unique: Boolean = false, primary: Boolean = false): Index {
        if (primary) assert(primaryKey == null) { "The primary key already exists in this table" }
        val indexName =
            when {
                name != null -> name
                primary -> this.name + "_pk"
                unique -> this.name + "_ux_" + (++indexCounter)
                else -> this.name + "_ie_" + (++indexCounter)
            }
        val index = Index(this, indexName, indexColumns.asList(), unique = unique, primary = primary)
        registerInnerElement(index)
        indices += index
        if (primary) primaryKey = index
        return index
    }

    fun newForeignKeyAndColumns(refKey: Index, columnPrefix: String?, mandatory: Boolean = false, cascadeDelete: Boolean = false): ForeignKey {
        val domColumns = ArrayList<Column>(refKey.columns.size)
        for (refColumn in refKey.columns) {
            val domColumn = newColumn(refKey.major.wordRoot, refColumn, mandatory)
            domColumns += domColumn
        }
        return newForeignKey(refKey, domColumns, cascadeDelete)
    }

    fun newForeignKey(refKey: Index, domColumns: List<Column>, cascadeDelete: Boolean = false): ForeignKey {
        val refTable = refKey.major as Table
        val wr = "${this.wordRoot}_${refTable.wordRoot}_fk"
        val name = if (area?.prefix != null) area.prefix + '_' + wr else wr
        return newForeignKey(name, refKey, domColumns, cascadeDelete)
    }

    fun newForeignKey(name: String, refKey: Index, domColumns: List<Column>, cascadeDelete: Boolean = false): ForeignKey {
        val fk = ForeignKey(this, name, refKey, domColumns, cascadeDelete)
        registerInnerElement(fk)
        foreignKeys += fk
        return fk
    }


    val primaryColumns: List<Column> =
        primaryKey?.columns ?: emptyList()

    override fun toString(): String = "$name: $role (${columns.joinToString { it.name!! }})"
}


class Column : MinorElement {

    var primaRef: Column? = null
    var ownType: Type? = null
    var mandatory: Boolean = false

    constructor(major: MajorObject, name: String, primaRef: Column, mandatory: Boolean = false) : super(major, name) {
        this.primaRef = primaRef
        this.mandatory = mandatory
    }

    constructor(major: MajorObject, name: String, type: Type, mandatory: Boolean = false) : super(major, name) {
        this.ownType = type
        this.mandatory = mandatory
    }


    val type: Type
        get() = ownType ?: primaRef?.type ?: BoolType

    override fun toString(): String = "$name: $type"
}


class Index : MinorElement {

    val columns: List<Column>

    val unique: Boolean
    val primary: Boolean

    constructor(table: Table, name: String?, columns: List<Column>, unique: Boolean = false, primary: Boolean = false) : super(table, name) {
        this.columns =
            when (columns.size) {
                0    -> throw IllegalArgumentException("Columns are missing in the index $name")
                1    -> Collections.singletonList(columns[0])
                else -> ArrayList(columns)
            }
        this.unique = unique || primary
        this.primary = primary
    }

    override fun toString(): String = "$name (${columns.joinToString {it.name ?: "<unnamed>"}})"
}


class ForeignKey : MinorElement {

    val refKey: Index
    val domColumns: List<Column>
    val cascadeDelete: Boolean

    constructor(table: Table, name: String, refKey: Index, domColumns: List<Column>, cascadeDelete: Boolean = false) : super(table, name) {
        assert(refKey.columns.size == domColumns.size) { "Cannot create a foreign key: column numbers don't match" }
        this.refKey = refKey
        this.domColumns = domColumns
        this.cascadeDelete = cascadeDelete
    }
}

