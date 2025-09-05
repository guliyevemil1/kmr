package org.guliyevemil.kmr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch

interface Sourcer<T> {
    val source: Flow<T>
}

class FlowSourcer<T>(f: Flow<T>) : Sourcer<T> {
    override val source: Flow<T> = f
}

class IterableSourcer<T>(
    collection: Iterable<T>,
    name: String? = null,
) : Sourcer<T> {
    constructor(vararg l: T, name: String? = null) : this(l.toList(), name)

    override val source: Flow<T> = flow {
        for (v in collection) {
            emit(v)
        }
    }
}

fun <T : Any> CoroutineScope.source(
    sourcer: Sourcer<T>,
    name: String? = null,
    size: Int = DEFAULT_SIZE,
    capacity: Int = DEFAULT_CAPACITY,
): ParallelChannel<T> {
    val chan = ParallelChannel<T>(name, size, capacity, this)
    launch {
        sourcer.source.collect { chan.send(data = it) }
        chan.close()
    }
    return chan
}

fun <A : Any, B : Any> CoroutineScope.combine2(
    ch1: ParallelChannel<A>,
    ch2: ParallelChannel<B>,
    name: String? = null,
    size: Int = DEFAULT_SIZE,
    capacity: Int = DEFAULT_CAPACITY,
): ParallelChannel<Either<A, B>> {
    val out = ParallelChannel<Either<A, B>>(name, size, capacity, this)
    launch { ch1.map<Either<A, B>> { Either.Left(it) }.reroute(out) }
    launch { ch2.map<Either<A, B>> { Either.Right(it) }.reroute(out) }
    return out
}

fun <T : Any> CoroutineScope.combine(
    vararg chans: ParallelChannel<T>,
    name: String? = null,
    size: Int = DEFAULT_SIZE,
    capacity: Int = DEFAULT_CAPACITY,
): ParallelChannel<T> {
    val out = ParallelChannel<T>(name, size, capacity, this)
    for (s in chans) {
        launch { s.reroute(out) }
    }
    return out
}
