package lb.crazydb.huge

import lb.crazydb.Model
import lb.crazydb.Producer


object HugeMaster {

    fun generate() {
        val dict = Dictionary()
        val model = Model()
        val contriver = HugeContriver(model, dict, "hg")
        contriver.inventCrazySchema(3, 20)
        val producer = Producer(model)
        producer.produceWholeScript()
    }

}