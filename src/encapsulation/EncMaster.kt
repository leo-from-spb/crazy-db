package lb.crazydb.encapsulation

import lb.crazydb.Model
import lb.crazydb.gears.Dictionaries


class EncMaster (val model: Model) {

    val tasks = arrayOf(
            EncTask("enc1", "huge", 3)
    )


    fun generate() {
        println("Generating 'Encapsulation'…")

        for (task in tasks) generateOneArea(model, task)
    }


    fun generateOneArea(model: Model, task: EncTask) {
        val dict = Dictionaries.obtain(task.dictionaryFolderName)
        val contriver = EncContriver(model, dict, task.areaPrefix)
        contriver.inventSchema(task.filesNumber, task.tablesPerFile, task.dataColumnsLim)
    }



}