@file:JvmName("ModelThings")

package lb.crazy.model


enum class TableRole {
    Master,
    Detail,
    ManyToMany,
    Dictionary,
    DictionaryDetail
}
