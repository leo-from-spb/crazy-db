package lb.crazydb.gears

import java.lang.StringBuilder


val consonants: Set<Char>
        = setOf('B','C','D','F','G','H','J','K','L','M','N','P','Q','R','S','T','V','W','X','Z',
                'b','c','d','f','g','h','j','k','l','m','n','p','q','r','s','t','v','w','x','z')
val vowels: Set<Char>
        = setOf('A','E','I','O','U','Y',
                'a','e','i','o','u','y')


fun String.abb(len: Int = 3): String {
    val b = StringBuilder(this)
    for (i in this.length - 1 downTo 0) if (b[i] !in consonants) b.delete(i, i+1)
    return b.substring(0, Math.min(len, b.length))
}

