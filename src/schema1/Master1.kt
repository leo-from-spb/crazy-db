package lb.crazydb.schema1

import lb.crazydb.Model
import lb.crazydb.Producer


object Master1 {

    fun generate() {
        val dict = Dictionary()
        val model = Model()
        val contriver = Contriver(model, dict, "cr1")
        contriver.inventCrazySchema(3, 20)
        val producer = Producer(model)
        producer.produceWholeScript()
    }

}