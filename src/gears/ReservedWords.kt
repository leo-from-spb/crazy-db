package lb.crazydb.gears


object ReservedWords {

    val words: Set<String>


    init {
        val loader = WordLoader("./dict")
        val list = loader.loadWords("reserved.txt")
        words = HashSet(list)
        print("Found ${words.size} reserved words\n")
    }


}