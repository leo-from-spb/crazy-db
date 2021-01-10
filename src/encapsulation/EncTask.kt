package lb.crazydb.encapsulation

data class EncTask (
        val areaPrefix: String,
        val dictionaryFolderName: String,
        val filesNumber: Int,
        val tablesPerFile: Int,
        val dataColumnsLim: Int
)
