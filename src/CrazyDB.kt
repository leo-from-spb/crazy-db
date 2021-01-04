package lb.crazydb

import lb.crazydb.encapsulation.EncMaster
import lb.crazydb.huge.HugeMaster


object CrazyDB {

    @JvmStatic
    fun main(args: Array<String>) {
        val model = Model()

        HugeMaster(model).generate()
        EncMaster(model).generate()

        val producer = Producer(model)
        producer.produceWholeScript()
    }

}