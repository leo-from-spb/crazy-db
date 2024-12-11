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
    

}