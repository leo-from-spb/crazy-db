package lb.crazydb.huge

import lb.crazydb.gears.GenerationTask


data class HugeTask(

        override val areaPrefix: String,
        override val dictionaryFolderName: String,
        override val filesNumber: Int = 1,
        val portionsNumber: Int = 10

) : GenerationTask

