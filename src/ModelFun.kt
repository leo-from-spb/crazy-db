package lb.crazydb


data class NameSpec(val name: String, val spec: String)



fun Model.newSequence(vararg nameWords: String): Sequence {
    val sequence = Sequence(*nameWords)
    this.sequences += sequence
    this.order += sequence
    return sequence
}


fun Model.newTable(role: TableRole, vararg nameWords: String): Table {
    val table = Table(role, *nameWords)
    this.tables += table
    this.order += table
    return table
}


fun Model.newView(vararg nameWords: String): View {
    val view = View(*nameWords)
    this.views += view
    this.order += view
    return view
}


fun Table.newColumn(vararg nameWords: String): TableColumn {
    return TableColumn(this, *nameWords)
}

fun Table.newColumn(nameSpec: NameSpec): TableColumn {
    val column = this.newColumn(nameSpec.name)
    column.setDataTypeAndDefault(nameSpec.spec)
    return column
}


