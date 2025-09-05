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

    fun <A : Any, B : Any> apply(x: Tuple2<A>, f: U.(A) -> B): Tuple2<B> = Tuple2(
        first.f(x.first),
        second.f(x.second),
    )
}

data class Tuple3<U>(
    val first: U,
    val second: U,
    val third: U,
) {
    constructor(u: () -> U) : this(u(), u(), u())

    fun <A : Any, B : Any> apply(x: Tuple3<A>, f: U.(A) -> B): Tuple3<B> = Tuple3(
        first.f(x.first),
        second.f(x.second),
        third.f(x.third),
    )
}

data class Tuple4<U>(
    val first: U,
    val second: U,
    val third: U,
    val fourth: U,
) {
    constructor(u: () -> U) : this(u(), u(), u(), u())

    fun <A : Any, B : Any> apply(x: Tuple4<A>, f: U.(A) -> B): Tuple4<B> = Tuple4(
        first.f(x.first),
        second.f(x.second),
        third.f(x.third),
        fourth.f(x.fourth),
    )
}

data class Tuple5<U>(
    val first: U,
    val second: U,
    val third: U,
    val fourth: U,
    val fifth: U,
) {
    constructor(u: () -> U) : this(u(), u(), u(), u(), u())

    fun <A : Any, B : Any> apply(x: Tuple5<A>, f: U.(A) -> B): Tuple5<B> = Tuple5(
        first.f(x.first),
        second.f(x.second),
        third.f(x.third),
        fourth.f(x.fourth),
        fifth.f(x.fifth),
    )
}
