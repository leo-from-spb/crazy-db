package lb.crazydb.huge

import lb.crazydb.gears.WordLoader
import lb.crazydb.gears.panic
import java.nio.file.Files
import java.nio.file.Path
import kotlin.random.Random


class Dictionary (val folderName: String) {

    val nouns: List<String>
    val verbs: List<String>
    val adjectives: List<String>

    private val rnd: Random


    companion object {
        val dictionariesPath: Path = Path.of("./dict")

        init {
            assert(Files.isDirectory(dictionariesPath)) { "The path $dictionariesPath should be a directory with dictionaries" }
        }
    }


    init {

        val dictionaryPath = dictionariesPath.resolve(folderName)

        val loader = WordLoader(dictionaryPath)

        println("Loading dictionary:")

        nouns = loader.loadWords("nouns.txt")
        verbs = loader.loadWords("verbs.txt")
        adjectives = loader.loadWords("adjectives.txt")

        println("""|nouns:      ${nouns.size}
                   |verbs:      ${verbs.size}
                   |adjectives: ${adjectives.size}
                """.trimMargin())

        rnd = Random(System.currentTimeMillis() xor System.nanoTime())
    }


    fun guessNoun(min: Int, vararg except: Set<String>): String = guessWord(nouns, min, *except)
    fun guessVerb(min: Int, vararg except: Set<String>): String = guessWord(verbs, min, *except)
    fun guessAdjective(min: Int, vararg except: Set<String>): String = guessWord(adjectives, min, *except)


    fun guessWord(list: List<String>, min: Int, vararg except: Set<String>): String {
        val n = list.size
        assert(n >= 3)

        // attempt 1 - quick random
        for (attempt in 1 .. 100) {
            val x = rnd.nextInt(n)
            val word = list[x]
            if (word.length >= min && word.isNotIn(*except)) return word
        }

        // attempt 2 - scan
        for (word in list) {
            if (word.length >= min && word.isNotIn(*except)) return word
        }

        // no more words
        panic("No more words!")
    }


    fun String.isIn(vararg sets: Set<String>): Boolean {
        for (set in sets) if (this in set) return true
        return false
    }

    fun String.isNotIn(vararg sets: Set<String>): Boolean = !this.isIn(*sets)



}
