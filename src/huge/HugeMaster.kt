package lb.crazydb.huge

import lb.crazydb.Model
import lb.crazydb.Producer
import lb.crazydb.gears.Dictionaries


object HugeMaster {

    val tasks = arrayOf(
            HugeTask("hg1", "huge", 1, 10),
            HugeTask("hg2", "huge", 2, 15),
            HugeTask("hg3", "huge", 3, 25)
    )


    fun generate() {
        val model = Model()

        for (task in tasks) generateOneArea(model, task)

        val producer = Producer(model)
        producer.produceWholeScript()
    }

    
    fun generateOneArea(model: Model, task: HugeTask) {
        val dict = Dictionaries.obtain(task.dictionaryFolderName)
        val contriver = HugeContriver(model, dict, task.areaPrefix)
        contriver.inventCrazySchema(task.filesNumber, task.portionsNumber)
    }

}