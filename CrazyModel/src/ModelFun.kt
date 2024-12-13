@file:JvmName("ModelFun")

package lb.crazy.model


fun <E: NamedEntity> List<E>.byNames(vararg names: String): List<E> =
    names.mapNotNull { this.find { e -> e.name == it } }
