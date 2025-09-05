package org.guliyevemil.example

import kotlinx.coroutines.runBlocking
import org.guliyevemil.kmr.IterableSourcer
import org.guliyevemil.kmr.source

fun main(args: Array<String>) {
    runBlocking {
        source(IterableSourcer<Int>(1..100))
            .map("square each number") { it * it }
            .filter("filter multiples of 5") { it % 5 > 0 }
            .batchMap { it.size to it }
            .forEach { println("got a batch $it") }
            .forEach("print sums of batches") { println("batched of size ${it.first} had a sum of ${it.second.sum()}") }
            .sink()
    }
    println("Done")
}
