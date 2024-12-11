@file:JvmName("NumberFun")

package lb.crazy.model

import kotlin.math.max


val Int.numberOfDigits: Int
    get() {
        var x = if (this >= 0) this else -this
        var count = 0
        do {
            x /= 10
            count++
        }
        while (x > 0)
        return count
    }



val IntRange.maxWing: Int
    get() = max(-first.coerceAtMost(0), last.coerceAtLeast(0))



fun <T> Boolean.choose(whenTrue: T, whenFalse: T): T = if (this) whenTrue else whenFalse

