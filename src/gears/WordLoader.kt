package lb.crazydb.gears

import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.streams.toList


class WordLoader (

    val rootDir: Path

) {

    companion object {
        val normalWordPattern = Regex("^[A-Za-z][A-Za-z_]*[A-Za-z]$")
    }

    constructor(rootPath: String) : this(Path.of(rootPath))

    init {
        assert(Files.exists(rootDir))
    }

    fun loadWords(fileName: String, pattern: Regex = normalWordPattern): List<String> {
        val file = rootDir.resolve(fileName)
        assert(Files.exists(file)) { "File $file doesn't exists." }
        val list1 = Files.lines(file)
            .filter(pattern::matches)
            .toList()
        val set = TreeSet(String.CASE_INSENSITIVE_ORDER)
        set.addAll(list1)
        return ArrayList(set)
    }


}