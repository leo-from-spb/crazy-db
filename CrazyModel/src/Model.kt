package lb.crazy.model

import java.util.*


class Model (val settings: ModelSettings) {

    val schemas = ArrayList<Schema>()

    val schemasByName: MutableMap<String, Schema> = HashMap()

    fun newSchema(name: String): Schema {
        assert(name !in schemasByName.keys)
        val schema = Schema(this, name)
        schemas += schema
        schemasByName[name] = schema
        return schema
    }

    fun obtainSchema(name: String): Schema =
        schemasByName[name]
        ?:
        newSchema(name)
    
}


sealed class Entity (val model: Model) {

    abstract val parent: Entity?

}


sealed class NamedEntity (model: Model, val name: String): Entity(model) {

    val innerNames = HashSet<String>()

    init {
        assert(name.isNotEmpty()) { "The name is empty" }
        assert(name.length <= model.settings.nameLengthLimit) { "The name \"$name\" is too long (the limit is ${model.settings.nameLengthLimit} characters)" }
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
    val views = ArrayList<View>()

    val areasByName: MutableMap<String, SubjectArea> = LinkedHashMap()

    val boringArea: SubjectArea

    constructor(model: Model, name: String) : super(model, name) {
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


class SubjectArea (val schema: Schema, val prefix: String?): Entity(schema.model) {

    override val parent: Entity
        get() = schema

    val rootWords: MutableMap<String, MajorObject> = HashMap()

    val tables = ArrayList<Table>()
    val views = ArrayList<View>()

    fun newTable(rootWord: String, role: TableRole): Table {
        assert (rootWord !in rootWords.keys)
        val table = Table(this, rootWord, role)
        tables += table
        schema.tables += table
        return table
    }

    fun newView(rootWord: String): View {
        assert (rootWord !in rootWords.keys)
        val view = View(this, rootWord)
        views += view
        schema.views += view
        return view
    }

    override fun toString(): String = "$prefix (${tables.size} tables)"

}


sealed class MajorObject : NamedEntity {

    val area: SubjectArea?
    val wordRoot: String?

    override val parent: Entity?
        get() = area

    constructor(area: SubjectArea, wordRoot: String) : super(area.model, nameOf(area, wordRoot)) {
        this.area = area
        this.wordRoot = wordRoot
    }
}


sealed class MinorElement : NamedEntity {

    val major: MajorObject

    override val parent: Entity?
        get() = major

    constructor(major: MajorObject, name: String) : super(major.model, name) {
        this.major = major
    }

}


private fun nameOf(area: SubjectArea, rootWord: String): String =
    when {
        area.prefix == null -> rootWord
        else -> area.prefix + '_' + rootWord
    }



sealed class LikeTable<C: Column> : MajorObject {

    val columns = ArrayList<C>()
    val columnsByName = HashMap<String, C>()

    protected var indexCounter = 0

    constructor(area: SubjectArea, wordRoot: String) : super(area, wordRoot)

    protected fun registerColumn(column: C) {
        registerInnerElement(column)
        columns += column
        columnsByName[column.name!!] = column
    }

}



class Table : LikeTable<TableColumn> {

    val role: TableRole

    val indices = ArrayList<Index>()
    val foreignKeys = ArrayList<ForeignKey>()

    var primaryKey: Index? = null
        private set

    constructor(area: SubjectArea, rootWord: String, role: TableRole) : super(area, rootWord) {
        this.role = role
    }

    fun newColumn(columnPrefix: String?, primaRef: Column, mandatory: Boolean = false): TableColumn {
        val name = adjustIdentifierBySize(primaRef.name?.withPrefix(columnPrefix, '_'), limit = model.settings.nameLengthLimit) ?: "column_${columns.size + 1}"
        assert (name !in columnsByName.keys) { "The name $name is already used by another column" }
        val column = TableColumn(this, name, primaRef, mandatory)
        registerColumn(column)
        return column
    }

    fun newColumn(name: String, type: Type, mandatory: Boolean = false): TableColumn {
        assert (name !in columnsByName.keys)
        val column = TableColumn(this, name, type, mandatory)
        registerColumn(column)
        return column
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
            val domColumn = newColumn(refKey.table.wordRoot, refColumn, mandatory)
            domColumns += domColumn
        }
        return newForeignKey(refKey, domColumns, cascadeDelete)
    }

    fun newForeignKey(refKey: Index, domColumns: List<Column>, cascadeDelete: Boolean = false): ForeignKey {
        val refTable = refKey.table
        val wr = "${this.wordRoot}_${refTable.wordRoot}_fk"
        var name = if (area?.prefix != null) area.prefix + '_' + wr else wr
        name = adjustIdentifierBySize(name, limit = model.settings.nameLengthLimit) 
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

    override fun toString(): String = "table $name: $role (${columns.joinToString { it.name!! }})"
}


class View : LikeTable<ViewColumn> {

    val sections = ArrayList<ViewTableSection>()

    constructor(area: SubjectArea, wordRoot: String) : super(area, wordRoot)

    fun addSection(alias: Char?, table: Table, binds: Collection<ForeignKey>?, columns: List<Column>) {
        if (binds != null)
            for (bind in binds)
                assert(sections.any { it.table === bind.refKey.table })
        val filteredColumns =
            if (sections.isEmpty()) columns
            else columns.filter { it.name !in this.columnsByName.keys }
        val section = ViewTableSection(alias, table, binds ?: emptySet(), filteredColumns)
        val ownColumns = filteredColumns.map { ViewColumn(this, section, it) }
        sections += section
        for (column in ownColumns) registerColumn(column)
    }

    override fun toString() = "view $name for tables ${sections.joinToString { it.table.name }}"
}


class ViewTableSection (val alias: Char?, val table: LikeTable<*>, val binds: Collection<ForeignKey>, val columns: List<Column>) {

    val aliasOrName
        get() = alias?.toString() ?: table.name

}



sealed class Column : MinorElement {

    companion object {
        private fun computeColumnNumber(table: LikeTable<*>): Int = table.columns.size + 1
    }

    val table: LikeTable<*>
    val nr: Int

    var primaRef: Column? = null
    var ownType: Type? = null
    var mandatory: Boolean = false

    constructor(table: LikeTable<*>, name: String, primaRef: Column, mandatory: Boolean = false) : super(table, name) {
        this.table = table
        this.nr = computeColumnNumber(table)
        this.primaRef = primaRef
        this.mandatory = mandatory
    }

    constructor(table: LikeTable<*>, name: String, type: Type, mandatory: Boolean = false) : super(table, name) {
        this.table = table
        this.nr = computeColumnNumber(table)
        this.ownType = type
        this.mandatory = mandatory
    }

    val type: Type
        get() = ownType ?: primaRef?.type ?: BoolType

    override fun toString(): String = "$name: $type"
}


class TableColumn : Column {

    var defaultExpression: String? = null

    constructor(table: Table, name: String, primaRef: Column, mandatory: Boolean)
            : super(table, name, primaRef, mandatory)

    constructor(table: Table, name: String, type: Type, mandatory: Boolean)
            : super(table, name, type, mandatory)

}


class ViewColumn : Column {

    val section: ViewTableSection

    constructor(view: View, section: ViewTableSection, primaRef: Column)
            : super(view, primaRef.name, primaRef)
    {
        this.section = section
    }

    val nameWithAlias: String
        get() {
            val alias = section.alias
            return if (alias != null) "$alias.$name" else name
        }

    val nameWithAliasOrTableName: String
        get() {
            val alias = section.alias
            return if (alias != null) "$alias.$name" else "${section.table.name}.$name"
        }


}



class Index : MinorElement {

    val table: Table
    val columns: List<Column>

    val unique: Boolean
    val primary: Boolean

    constructor(table: Table, name: String, columns: List<Column>, unique: Boolean = false, primary: Boolean = false) : super(table, name) {
        this.table = table
        this.columns =
            when (columns.size) {
                0    -> throw IllegalArgumentException("Columns are missing in the index $name")
                1    -> Collections.singletonList(columns[0])
                else -> ArrayList(columns)
            }
        this.unique = unique || primary
        this.primary = primary
    }

    val columnNames: String
        get() =
            if (columns.size == 1) columns[0].name!!
            else columns.joinToString { it.name!! }

    override fun toString(): String = "$name (${columns.joinToString {it.name ?: "<unnamed>"}})"
}


class ForeignKey : MinorElement {

    val refKey: Index
    val domColumns: List<Column>
    val cascadeDelete: Boolean

    val domColumnNames: String

    constructor(table: Table, name: String, refKey: Index, domColumns: List<Column>, cascadeDelete: Boolean = false) : super(table, name) {
        assert(refKey.columns.size == domColumns.size) { "Cannot create a foreign key: column numbers don't match" }
        this.refKey = refKey
        this.domColumns = domColumns
        this.domColumnNames = domColumns.nameStr()
        this.cascadeDelete = cascadeDelete
    }

}


////// UTILITY FUNCTIONS \\\\\\


fun <E: NamedEntity> List<E>.byNames(vararg names: String): List<E> =
    names.mapNotNull { this.find { e -> e.name == it } }


fun <E: NamedEntity> List<E>.names(): Array<String> =
    this.map { it.name }.toTypedArray()

fun <E: NamedEntity> List<E>.nameStr(): String =
    when (this.size) {
        0 -> ""
        1 -> this[0].name
        else -> this.joinToString { it.name }
    }