package lb.crazy.dedal.dict

import java.nio.file.Files
import java.nio.file.Path

/**
 * Service that loads word dictionaries.
 * @property dir — path to the directory with dictionaries.
 */
class WordLoader (val dir: Path) {


    companion object {
        val normalWordPattern = Regex("^[A-Za-z][A-Za-z_]*[A-Za-z]$")
    }


    fun loadBook(): WordBook {
        val nouns = loadDictionary(WordClass.Noun)
        val verbs = loadDictionary(WordClass.Verb)
        val adjectives = loadDictionary(WordClass.Adjective)
        return WordBook(nouns, verbs, adjectives)
    }


    fun loadDictionary(wordClass: WordClass, fileName: String? = null): WordDictionary {
        val fName = (fileName ?: (wordClass.toString().lowercase()+'s')) + ".txt"
        val fPath = dir.resolve(fName)
        assert(Files.exists(fPath)) { "File $fPath doesn't exists." }

        val theWords: List<String> = Files.lines(fPath)
            .filter(normalWordPattern::matches)
            .sorted()
            .distinct()
            .collect(java.util.stream.Collectors.toList())

        return WordDictionary(wordClass, theWords)
    }

}