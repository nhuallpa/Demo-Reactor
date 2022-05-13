package com.nhuallpa.demoreactor

import org.junit.jupiter.api.Test
import org.reactivestreams.Subscription
import reactor.core.publisher.Flux
import java.time.Duration
import kotlin.random.Random
import org.reactivestreams.Subscriber as Subscriber

class FluxSubscribeTest {

    @Test
    fun `Create a flux and subscribe`() {
        Flux.just("A","B","C")
            .subscribe(System.out::println);
    }

    @Test
    fun `Mapping elements`() {

        Flux.range(2018, 5)
            .timestamp()
            .index()
            .subscribe { tuple -> System.out.println("Index: ${tuple.t1}, ts: ${tuple.t2.t1}, value: ${tuple.t2.t2}")}

    }


    @Test
    fun `Create a flux and custom subscribe`() {

        val stream : Flux<String> = Flux.just("Hello","Wold","!")
        stream.subscribe(CustomSubscriber());

    }

    @Test
    fun `Has evens`() {
        Flux.just(3,5,8,1,17,16,15)
            .any{ e -> e % 2 == 0}
            .subscribe { hasEvens -> System.out.println("Has evens: $hasEvens")}
    }

    @Test
    fun `Reduce elements`() {
        Flux.range(1, 5)
            .reduce(0) { acc, elem -> acc + elem }
            .subscribe { result -> System.out.println("Result: $result")}
    }


    @Test
    fun `Then many`() {
        Flux.just(1,2,3)
            .thenMany(Flux.just(4,5))
            .subscribe {e -> System.out.println("onNext: $e")}
    }

    @Test
    fun `Concat flux`() {
        Flux.concat(
            Flux.range(1, 3),
            Flux.range(4,2),
            Flux.range(6, 5)
        ).subscribe { e -> System.out.println("onNext: $e")}
    }


    @Test
    fun `Find users`() {
        Flux.just("user-1","user-2","user-3")
            .flatMap { u -> requestsBooks(u).map { b -> "$u/$b" } }
            .subscribe { r -> System.out.println("onNext: $r")}
    }

    fun recommendedBooks(userId: String): Flux<String>  {
        return Flux.defer{
            if (Random.nextInt(10) < 7) {
                Flux.error<String>(RuntimeException("Err")).delayElements(Duration.ofMillis(100))
            } else {
                Flux.just("Blue marks", "The expanse").delayElements(Duration.ofMillis(50))
            }
        }.doOnSubscribe { System.out.println("Request for $userId") }
    }

    class CustomSubscriber : Subscriber<String> {

        @Volatile
        private var subscription: Subscription? = null

        override fun onSubscribe(s: Subscription?) {
            subscription = s
            System.out.println("Initial request")
            subscription?.request(1)
        }

        override fun onNext(t: String?) {
            System.out.println("On Next $t")
            System.out.println("requesting 1 element")
            subscription?.request(1)
        }

        override fun onError(t: Throwable?) {
            if (t != null) {
                System.out.println("On Error ${t.message}")
            }
        }

        override fun onComplete() {
            System.out.println("On Complete")
        }

    }

    fun requestsBooks(user: String) : Flux<String> {
        return Flux.range(1, Random.nextInt(3) + 1)
            .map { i -> "book-$i"}
            .delayElements(Duration.ofMillis(3))
    }

}