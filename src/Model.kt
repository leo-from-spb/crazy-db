package lb.crazydb

import lb.crazydb.gears.ReservedWords


class Model {

    val tables = ArrayList<Table>()

    val usedNames = HashSet<String>(1000000)


    init {
        usedNames.addAll(ReservedWords.words)
    }

}





class Table (val name: String) {

    val columns = ArrayList<Column>()
    val dependentOnTables: Set<Table> = emptySet()

    val primaryKeySize: Int
        get() = columns.stream().filter(Column::primary).count().toInt()

}


class View (val name: String) {
    val columns = ArrayList<Column>()
    val baseTables = ArrayList<Table>()
}


class Column (val table: Table, val name: String) {

    var mandatory = false
    var primary = false
    var dataType: String = "char"

    init {
        table.columns += this
    }
    
}



