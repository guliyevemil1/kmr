package org.guliyevemil.kmr

sealed class Either<A, B> {
    fun isLeft(): Boolean = this is Left
    fun isRight(): Boolean = this is Right
    fun left(): Left<A, B>? = this as? Left
    fun right(): Right<A, B>? = this as? Right
    fun <C> mapLeft(f: (A) -> C): Either<C, B> = when (this) {
        is Left -> Left(f(this.value))
        is Right -> Right(this.value)
    }

    fun <C> mapRight(f: (B) -> C): Either<A, C> = when (this) {
        is Left -> Left(this.value)
        is Right -> Right(f(this.value))
    }

    data class Left<A, B>(val value: A) : Either<A, B>()
    data class Right<A, B>(val value: B) : Either<A, B>()
}

data class Tuple2<U>(
    val first: U,
    val second: U,
) {
    constructor(u: () -> U) : this(u(), u())
}

data class Tuple3<U>(
    val first: U,
    val second: U,
    val third: U,
) {
    constructor(u: () -> U) : this(u(), u(), u())
}

data class Tuple4<U>(
    val first: U,
    val second: U,
    val third: U,
    val fourth: U,
) {
    constructor(u: () -> U) : this(u(), u(), u(), u())
}

data class Tuple5<U>(
    val first: U,
    val second: U,
    val third: U,
    val fourth: U,
    val fifth: U,
) {
    constructor(u: () -> U) : this(u(), u(), u(), u(), u())
}
