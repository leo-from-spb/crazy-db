package lb.crazydb.huge


data class HugeTask(
        val areaPrefix: String,
        val dictionaryFolderName: String,
        val filesNumber: Int = 1,
        val portionsNumber: Int = 10
)

