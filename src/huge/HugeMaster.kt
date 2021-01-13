package lb.crazydb.huge

import lb.crazydb.Model
import lb.crazydb.gears.Dictionaries


class HugeMaster (val model: Model) {

    fun generate(tasks: Array<HugeTask>) {
        println("Generating 'Huge'…")
        for (task in tasks) generateOneArea(model, task)
    }

    
    fun generateOneArea(model: Model, task: HugeTask) {
        val dict = Dictionaries.obtain(task.dictionaryFolderName)
        val contriver = HugeContriver(model, dict, task.areaPrefix)
        contriver.inventCrazySchema(task.filesNumber, task.portionsNumber, task.withSynonyms)
    }

}