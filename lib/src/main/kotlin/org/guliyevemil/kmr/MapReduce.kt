package org.guliyevemil.kmr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

const val DEFAULT_SIZE = 10
const val DEFAULT_CAPACITY = 10

class ParallelChannel<T : Any> internal constructor(
    val name: String? = null,
    val size: Int,
    val capacity: Int,
    private val scope: CoroutineScope,
) {
    init {
        require(size > 0) { "size must be greater than 0" }
    }

    val channels: List<Channel<T>> = List<Channel<T>>(size) { Channel(capacity = capacity) }

    private fun <U : Any> newParallelChannel(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        scope: CoroutineScope = this.scope,
    ) = ParallelChannel<U>(name, size, capacity, scope)

    internal suspend fun send(data: T) = send(data, data.hashCode())

    internal suspend fun send(data: T, shard: Int) {
        var n = shard % channels.size
        while (n < 0) {
            n += channels.size
        }
        channels[n].send(data)
    }

    fun <U : Any> map(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (T) -> U,
    ): ParallelChannel<U> =
        map(name, size, capacity) { _, input -> f(input) }

    fun <U : Any> map(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (Int, T) -> U,
    ): ParallelChannel<U> {
        val out = newParallelChannel<U>(name, size, capacity)
        applyRunners { shard, ch ->
            for (input in ch) {
                out.send(f(shard, input))
            }
        }
        return out
    }

    fun filter(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (T) -> Boolean,
    ): ParallelChannel<T> =
        filter(name, size, capacity) { _, input -> f(input) }

    fun filter(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (Int, T) -> Boolean,
    ): ParallelChannel<T> {
        val out = newParallelChannel<T>(name, size, capacity)
        applyRunners { shard, ch ->
            if (size == out.size) {
                for (input in ch) {
                    if (f(shard, input)) {
                        out.send(input, shard)
                    }
                }
            } else {
                for (input in ch) {
                    if (f(shard, input)) {
                        out.send(input)
                    }
                }
            }
        }
        return out
    }

    fun <U : Any> batchMap(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (List<T>) -> U,
    ): ParallelChannel<U> =
        batchMap(name, size, capacity) { _, input -> f(input) }

    fun <U : Any> batchMap(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (Int, List<T>) -> U,
    ): ParallelChannel<U> {
        val out: ParallelChannel<U> = newParallelChannel(name = name, size = size, capacity = capacity)
        applyRunners { chard, ch ->
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
        return out
    }

    fun <U : Any> reduce(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (T) -> U,
    ): ParallelChannel<U> =
        map(name, size, capacity) { _, input -> f(input) }

    fun <U : Any> multiMap2(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (T) -> Tuple2<U>,
    ): Tuple2<ParallelChannel<U>> {
        val out = Tuple2 { newParallelChannel<U>(name, size, capacity) }
        applyRunners { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    fun <U : Any> multiMap3(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: (T) -> Tuple3<U>,
    ): Tuple3<ParallelChannel<U>> {
        val out = Tuple3 { newParallelChannel<U>(name, size, capacity) }
        applyRunners { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    fun <U : Any> multiMap4(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: (T) -> Tuple4<U>,
    ): Tuple4<ParallelChannel<U>> {
        val out = Tuple4 { newParallelChannel<U>(name, size, capacity) }
        applyRunners { ch ->
            for (v in ch) {
                out.apply(f(v)) { runBlocking { send(it) } }
            }
        }
        return out
    }

    fun <U : Any> multiMap5(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: (T) -> Tuple5<U>,
    ): Tuple5<ParallelChannel<U>> {
        val out = Tuple5 { newParallelChannel<U>(name, size, capacity) }
        applyRunners { ch ->
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
        val out: ParallelChannel<T> = newParallelChannel(name, size, capacity)
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

    fun split(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
    ): Tuple2<ParallelChannel<T>> = multiMap2(
        name = name,
        size = size,
        capacity = capacity,
    ) { t -> Tuple2 { t } }

    fun split3(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
    ): Tuple3<ParallelChannel<T>> = multiMap3(
        name = name,
        size = size,
        capacity = capacity,
    ) { t -> Tuple3 { t } }

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
        applyRunners { ch ->
            while (true) {
                try {
                    ch.receive()
                } catch (_: Exception) {
                    break
                }
            }
        }
    }

    private fun applyRunners(runner: suspend (Channel<T>) -> Unit) {
        applyRunners { _, ch -> runner(ch) }
    }

    private fun applyRunners(runner: suspend (Int, Channel<T>) -> Unit) {
        val jobs: List<Job> = channels.mapIndexed { shard, ch ->
            scope.launch { runner(shard, ch) }
        }
        scope.launch {
            jobs.forEach { it.join() }
            close()
        }
    }

    fun close() {
        channels.forEach { it.close() }
    }

    fun reroute(
        out: ParallelChannel<T>,
        name: String? = null,
    ) {
        applyRunners { shard, ch ->
            if (size == out.size) {
                for (value in ch) {
                    out.send(value, shard)
                }
            } else {
                for (value in ch) {
                    out.send(value)
                }
            }
        }
    }
}
