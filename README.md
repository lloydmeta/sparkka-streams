# Sparkka-Streams [![Build Status](https://travis-ci.org/lloydmeta/sparkka-streams.svg?branch=master)](https://travis-ci.org/lloydmeta/sparkka-streams)

Need to use an Akka Flow to push elements into a Spark InputDStream? You may have all or any of the following reasons to want to do so:

1. You already have transformed Flows on your driver and you would like to tap into it at any point to power an InputDStream
2. Your `Source[A]` is not something you can or want to connect to from your Spark source (e.g. webcam, websocket, or 
   connection-limited API like Twitter Streaming)
3. For efficiency reasons, you want to maintain only 1 Source on your driver for many different Spark streaming transformations; 
   after all, more `Source` connections doesn't mean you will get data pipped to you any faster

Sparkka-Streams allows you to easily use an Akka Flow to power a Spark DStream, which comes in handy when you need to do streaming ML-related
things with Akka Streams.

## SBT

_Note_ only Scala 2.11+ is supported at the moment

```scala
libraryDependencies += "com.beachape" %% "sparkka-streams" % "1.5" 
```

## Usage/Example

__NOTE__ This library requires you to have Akka remote enabled; simply make sure your `application.conf` is [properly configured](http://doc.akka.io/docs/akka/snapshot/scala/remoting.html). 

```scala

/* 
 This can be done anywhere. You essentially get a Spark InputDStream and a Flow piece that you can plug into your Akka Flow 
 to feed the InputDStream
*/
val (inputDStream, feedDInput) = Streaming.connection[Int]()

val source = Source.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    
    val source = Source(1 to 10)
    
    val bCast = builder.add(Broadcast[Int](2))
    val merge = builder.add(Merge[Int](2))
    
    val add1 = Flow[Int].map(_ + 1)
    val times3 = Flow[Int].map(_ * 3)
    
    source ~> bCast ~> add1 ~> merge
              bCast ~> times3 ~> feedDInput ~> merge
    
    SourceShape(merge.out)
})

val reducedFlow = source.runWith(Sink.fold(0)(_ + _))
whenReady(reducedFlow)(_ shouldBe 230)

val sharedVar = ssc.sparkContext.accumulator(0)
inputDStream.foreachRDD { rdd =>
    rdd.foreach { i =>
      sharedVar += i
    }
}
ssc.start()
eventually(sharedVar.value shouldBe 165)
```

## Licence

The MIT License (MIT)

Copyright (c) 2016 by Lloyd Chan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
Status API Training Shop Blog About Pricing
