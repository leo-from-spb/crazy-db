package lb.crazy.dedal.generator

import lb.crazy.model.Model
import lb.crazy.model.Schema
import lb.crazy.model.getOrCreate
import kotlin.random.Random

/**
 *
 */
abstract class AbstractSchemaGenerator (val model: Model, schemaName: String) {

    val schema: Schema = model.obtainSchema(schemaName)


    abstract fun generate()


    protected val rnd = Random(System.nanoTime() * 17L)

}

