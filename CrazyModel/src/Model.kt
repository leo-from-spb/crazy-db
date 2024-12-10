package lb.crazy.model


class Model {

    val schemas = ArrayList<Schema>()
    


}


sealed class NamedEntity (val name: String) {



}


class Schema : NamedEntity {

    val tables = ArrayList<Table>()

    constructor(name: String) : super(name)

}


class Table : NamedEntity {

    val columns = ArrayList<Column>()

    constructor(name: String) : super(name)

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


