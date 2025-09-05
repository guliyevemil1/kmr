package org.guliyevemil.example

import kotlinx.coroutines.runBlocking
import org.guliyevemil.kmr.IterableSourcer
import org.guliyevemil.kmr.source

fun main(args: Array<String>) {
    runBlocking {
        source(IterableSourcer<Int>(1..100))
            .map { it * it }
            .filter { it % 5 > 0 }
            .map { it.toString() }
            .map { println(it) }
            .sink()
    }
    println("Done")
}
