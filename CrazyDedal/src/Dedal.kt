package lb.crazy.dedal

import lb.crazy.dedal.dict.WordLoader
import lb.crazy.dedal.generator.BasicSchemaGenerator
import lb.crazy.model.Model
import java.nio.file.Path
import kotlin.system.exitProcess

/**
 * Starting class for large DB generator.
 */
class Dedal {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            Dedal().run()
        }
    }


    private fun run() {
        say("Crazy Dedal is waking up!")

        val loader = WordLoader(Path.of("dict", "1"))
        val book = loader.loadBook()
        say("Loaded the dictionary book: ${book.nouns.size} nouns, ${book.verbs.size} verbs, ${book.adjectives.size} adjectives.")

        val model = Model()

        val generator = BasicSchemaGenerator(model, "Crazy_B", book)
        generator.generate()

        collectAndPrintModelStatistics(model)
    }


    private fun collectAndPrintModelStatistics(model: Model) {
        val nAreas = model.schemas.sumOf { it.areas.size }
        val nTables = model.schemas.sumOf { it.tables.size }
        val nColumns = model.schemas.sumOf { it.tables.sumOf { it.columns.size } }
        val nIndices = model.schemas.sumOf { it.tables.sumOf { it.indices.size } }
        val nForeignKeys = model.schemas.sumOf { it.tables.sumOf { it.foreignKeys.size } }

        val message = """|Model is generated.
                         |Statistics: 
                         |>schemas      : ${model.schemas.size}
                         |>subject areas: $nAreas
                         |>tables       : $nTables
                         |>columns      : $nColumns
                         |>indices      : $nIndices
                         |>foreign keys : $nForeignKeys
                      """.trimMargin().replace('>','\t')
        say(message)
    }



    fun say(message: String) {
        println(message)
    }


    fun panic(message: String): Nothing {
        System.err.println(message)
        System.err.println()
        exitProcess(100)
    }

}