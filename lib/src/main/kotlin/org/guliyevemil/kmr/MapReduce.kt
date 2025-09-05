package org.guliyevemil.kmr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch

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

    internal suspend fun send(shard: Int? = null, data: T) {
        var n = (shard ?: data.hashCode()) % channels.size
        while (n < 0) {
            n += channels.size
        }
        channels[n].send(data)
    }

    suspend fun send(inputShard: Int, f: Flow<T>, first: ParallelChannel<T>, vararg out: ParallelChannel<T>) {
        val shard: Int? = if (this.size == first.size && inputShard >= 0) {
            inputShard
        } else {
            null
        }
        f.collect { data ->
            first.send(shard, data)
            out.forEach { it.send(shard, data) }
        }
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
        applyRunners(out) { shard, ch ->
            for (input in ch) {
                out.send(data = f(shard, input))
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
        applyRunners(out) { shard, ch ->
            send(shard, flow {
                for (input in ch) {
                    if (f(shard, input)) {
                        emit(input)
                    }
                }
            }, out)
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
        applyRunners(out) { chard, ch ->
            val buffer = mutableListOf<T>()
            while (true) {
                try {
                    out.send(data = f(chard, readBatch(buffer, ch)))
                } catch (_: Exception) {
                    break
                }
            }
        }
        return out
    }

    fun forEach(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (T) -> Unit,
    ): ParallelChannel<T> =
        forEach(name, size, capacity) { _, input -> f(input) }

    fun forEach(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        f: suspend (Int, T) -> Unit,
    ): ParallelChannel<T> {
        val out = newParallelChannel<T>(name, size, capacity)
        applyRunners(out) { shard, ch ->
            for (input in ch) {
                f(shard, input)
                out.send(shard, input)
            }
        }
        return out
    }

    fun reduce(
        initialValue: T,
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
        groupBy: suspend (T) -> String,
        combine: suspend (T, T) -> T,
    ): ParallelChannel<T> {
        val out: ParallelChannel<T> = newParallelChannel(name, size, capacity)
        TODO()
    }

    fun resize(name: String? = null, size: Int = this.size, capacity: Int = this.capacity): ParallelChannel<T> {
        if (size == this.size && capacity == this.capacity) {
            return this
        }
        val out: ParallelChannel<T> = newParallelChannel(name, size, capacity)
        applyRunners(out) { _, ch -> send(-1, ch.receiveAsFlow(), out) }
        return out
    }

    fun split(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
    ): Tuple2<ParallelChannel<T>> {
        val out = Tuple2 { newParallelChannel<T>(name, size, capacity) }
        applyRunners(out.first, out.second) { shard, ch ->
            send(shard, ch.receiveAsFlow(), out.first, out.second)
        }
        return out
    }

    fun split3(
        name: String? = null,
        size: Int = this.size,
        capacity: Int = this.capacity,
    ): Tuple3<ParallelChannel<T>> {
        val out = Tuple3 { newParallelChannel<T>(name, size, capacity) }
        applyRunners(out.first, out.second, out.third) { shard, ch ->
            send(shard, ch.receiveAsFlow(), out.first, out.second, out.third)
        }
        return out
    }

    private suspend fun readBatch(out: MutableList<T>, ch: Channel<T>): List<T> {
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
        return out.toList()
    }

    fun sink() {
        applyRunners<Unit> { shard, ch ->
            while (true) {
                try {
                    ch.receive()
                } catch (_: ClosedReceiveChannelException) {
                    break
                }
            }
        }
    }

    private fun <U : Any> applyRunners(
        vararg out: ParallelChannel<U>,
        runner: suspend (Int, Channel<T>) -> Unit,
    ) {
        val jobs: List<Job> = channels.mapIndexed { shard, ch ->
            scope.launch { runner(shard, ch) }
        }
        if (out.isNotEmpty()) {
            scope.launch {
                jobs.forEach { it.join() }
                out.forEach { it.close() }
            }
        }
    }

    fun close() {
        channels.forEach { require(it.close()) }
    }

    fun reroute(
        out: ParallelChannel<T>,
        name: String? = null,
    ) {
        applyRunners(out) { shard, ch ->
            send(shard, ch.receiveAsFlow(), out)
        }
    }
}
