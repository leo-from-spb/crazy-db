package lb.crazy.dedal

import lb.crazy.dedal.dict.WordLoader
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