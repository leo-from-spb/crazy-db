package lb.crazydb.gears

import java.util.concurrent.ConcurrentHashMap

object Dictionaries {

    private val dicts = ConcurrentHashMap<String, Dictionary>()


    fun obtain(folderName: String): Dictionary {
        var d = dicts[folderName]
        if (d == null) {
            d = Dictionary(folderName)
            val existent = dicts.putIfAbsent(folderName, d)
            if (existent != null) d = existent
        }
        return d
    }

}