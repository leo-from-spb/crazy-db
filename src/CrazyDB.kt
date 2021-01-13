package lb.crazydb

import lb.crazydb.encapsulation.EncMaster
import lb.crazydb.encapsulation.EncTask
import lb.crazydb.gears.Dictionaries
import lb.crazydb.gears.GenerationTask
import lb.crazydb.huge.HugeMaster
import lb.crazydb.huge.HugeTask


object CrazyDB {

    val encTasks = arrayOf(
            EncTask("enc1", "huge", 7, 7, 12)
    )

    val hugeTasks = arrayOf(
            HugeTask("hg1", "huge", 1, 10),
            HugeTask("hg2", "huge", 2, 15),
            HugeTask("hg3", "huge", 3, 25, true)
    )


    @JvmStatic
    fun main(args: Array<String>) {
        prepare(encTasks)
        prepare(hugeTasks)

        val model = Model()

        val encMaster = EncMaster(model)
        encMaster.generate(encTasks)

        val hugeMaster = HugeMaster(model)
        hugeMaster.generate(hugeTasks)

        val producer = Producer(model)
        producer.produceWholeScript()
    }


    fun prepare(tasks: Array<out GenerationTask>) {
        for (task in tasks) {
            Dictionaries.obtain(task.dictionaryFolderName)
        }
    }

}