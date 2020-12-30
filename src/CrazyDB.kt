package lb.crazydb

import lb.crazydb.huge.HugeMaster


object CrazyDB {

    @JvmStatic
    fun main(args: Array<String>) {
        HugeMaster.generate()
    }

}