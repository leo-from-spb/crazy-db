package lb.crazy.dedal.dict

/**
 * Words of the same word class (part of speech).
 */
class WordDictionary {

    val wordClass: WordClass

    /**
     * Words, ordered alphabetically.
     * Immutable.
     */
    val words: List<String>

    /**
     * Number of words
     */
    val size: Int

    internal constructor(wordClass: WordClass, words: List<String>) {
        this.wordClass = wordClass
        this.words = words.toList()
        this.size = words.size
    }


    fun guessWord(min: Int, vararg except: Set<String>, capitalized: Boolean = false): String {
        var word = selectWord(min, *except)
        if (capitalized) word = word.replaceFirstChar(Char::uppercaseChar)
        return word
    }

    private fun selectWord(min: Int, vararg except: Set<String>): String {
        val n = size
        assert(n >= 3)

        // attempt 1 - quick random
        for (attempt in 1 .. 100) {
            val x = rnd.nextInt(n)
            val word = words[x]
            if (word.length >= min && word.isNotIn(*except)) return word
        }

        // attempt 2 - scan
        for (word in words) {
            if (word.length >= min && word.isNotIn(*except)) return word
        }

        // no more words
        throw Exception("No more words!")
    }


    fun String.isIn(vararg sets: Set<String>): Boolean {
        for (set in sets) if (this in set) return true
        return false
    }

    fun String.isNotIn(vararg sets: Set<String>): Boolean = !this.isIn(*sets)


    companion object {
        val rnd = java.util.Random(System.nanoTime())
    }


}