package lb.crazydb.encapsulation

import lb.crazydb.gears.GenerationTask

data class EncTask (

        override val areaPrefix: String,
        override val dictionaryFolderName: String,
        override val filesNumber: Int,
        val tablesPerFile: Int,
        val dataColumnsLim: Int

) : GenerationTask
