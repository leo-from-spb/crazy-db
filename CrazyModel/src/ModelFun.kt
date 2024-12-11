@file:JvmName("ModelFun")

package lb.crazy.model


inline fun <reified E: NamedEntity> MutableList<E>.getOrCreate(name: String, creating: () -> E): E {
    var entity = this.find { it.name == name }
    if (entity == null) {
        entity = creating()
        this.add(entity)
    }
    return entity
}


