package org.guliyevemil.kmr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

const val DEFAULT_SIZE = 10
const val DEFAULT_CAPACITY = 10

class ParallelChannel<T : Any> internal constructor(
    private val scope: CoroutineScope,
    val size: Int,
    val capacity: Int
) {

    private fun <U : Any> newParallelChannel(
        scope: CoroutineScope = this.scope,
        size: Int = this.size,
        capacity: Int = this.capacity,
    ) = ParallelChannel<U>(scope, size, capacity)

    val channels: List<Channel<T>> = List<Channel<T>>(size) { Channel(capacity = capacity) }

    init {
        require(size > 0) { "size must be greater than 0" }
    }

    internal suspend fun send(data: T) = send(data, data.hashCode())

    internal suspend fun send(data: T, shard: Int) {
        var n = shard % channels.size
        while (n < 0) {
            n += channels.size
        }
        channels[n].send(data)
    }

    fun <U : Any> map(name: String? = null, f: suspend (T) -> U): ParallelChannel<U> =
        map(name) { _, input -> f(input) }

    fun <U : Any> map(name: String? = null, f: suspend (Int, T) -> U): ParallelChannel<U> {
        val out = newParallelChannel<U>()
        val jobs = channels.mapIndexed { shard, ch ->
            scope.launch {
                for (input in ch) {
                    out.send(f(shard, input))
                }
            }
        }
        scope.launch {
            jobs.forEach { it.join() }
            out.close()
        }
        return out
    }

    fun <U : Any> batchMap(name: String? = null, f: suspend (List<T>) -> U): ParallelChannel<U> =
        batchMap(name) { _, input -> f(input) }

    fun <U : Any> batchMap(name: String? = null, f: suspend (Int, List<T>) -> U): ParallelChannel<U> {
        val out: ParallelChannel<U> = newParallelChannel()
        val jobs: List<Job> = channels.mapIndexed { chard, ch ->
            scope.launch {
                val batch = mutableListOf<T>()
                while (true) {
                    try {
                        readBatch(batch, ch)
                        out.send(f(chard, batch))
                    } catch (_: Exception) {
                        break
                    }
                }
            }
        }
        scope.launch {
            jobs.forEach { it.join() }
            out.close()
        }
        return out
    }

    suspend fun <U : Any> multiMap2(name: String? = null, f: suspend (T) -> Tuple2<U>): Tuple2<ParallelChannel<U>> {
        val out = Tuple2 { newParallelChannel<U>() }
        channels.forEach { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    suspend fun <U : Any> multiMap3(name: String? = null, f: (T) -> Tuple3<U>): Tuple3<ParallelChannel<U>> {
        val out = Tuple3 { newParallelChannel<U>() }
        channels.forEach { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    suspend fun <U : Any> multiMap4(name: String? = null, f: (T) -> Tuple4<U>): Tuple4<ParallelChannel<U>> {
        val out = Tuple4 { newParallelChannel<U>() }
        channels.forEach { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    suspend fun <U : Any> multiMap5(name: String? = null, f: (T) -> Tuple5<U>): Tuple5<ParallelChannel<U>> {
        val out = Tuple5 { newParallelChannel<U>() }
        channels.forEach { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    fun resize(name: String? = null, size: Int = this.size, capacity: Int = this.capacity): ParallelChannel<T> {
        if (size == this.size && capacity == this.capacity) {
            return this
        }
        val out: ParallelChannel<T> = newParallelChannel(size = size, capacity = capacity)
        val jobs = channels.map { ch ->
            scope.launch {
                for (value in ch) {
                    out.send(value)
                }
            }
        }
        scope.launch {
            jobs.forEach { it.join() }
            out.close()
        }
        return out
    }

    private suspend fun readBatch(out: MutableList<T>, ch: Channel<T>) {
        out.clear()
        // wait until we have at least one
        out.add(ch.receive())
        while (out.size < capacity) {
            val result = ch.tryReceive()
            if (result.isClosed) {
                break
            }
            out.add(result.getOrNull() ?: break)
        }
    }

    fun sink(name: String? = null) {
        channels.forEach { ch ->
            scope.launch {
                while (true) {
                    try {
                        ch.receive()
                    } catch (_: Exception) {
                        break
                    }
                }
            }
        }
    }

    fun close() {
        channels.forEach { it.close() }
    }

    fun reroute(name: String? = null, out: ParallelChannel<T>) {
        for (ch in channels) {
            scope.launch {
                for (value in ch) {
                    out.send(value)
                }
            }
        }
    }
}

