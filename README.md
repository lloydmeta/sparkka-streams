# Sparkka-Streams [![Build Status](https://travis-ci.org/lloydmeta/sparkka-streams.svg?branch=master)](https://travis-ci.org/lloydmeta/sparkka-streams)

Allows you to easily use an Akka Flow to power a Spark DStream, which comes in handy when you need to do streaming ML-related
things with Akka Streams.

## SBT

_Note_ only Scala 2.11+ is supported at the moment

```scala
libraryDependencies += "com.beachape" %% "sparkka-streams" % "1.1" 
```

## Usage

__NOTE__ This library requires you to have Akka remote enabled; simply make sure your `application.conf` is [properly configured](http://doc.akka.io/docs/akka/snapshot/scala/remoting.html). 

```scala
implicit val actorSystem = ActorSystem()
implicit val materializer = ActorMaterializer()
implicit val ssc = LocalContext.ssc

// InputDStream can then be used to build elements of the graph that require integration with Spark
val (inputDStream, feedDInput) = Streaming.streamConnection[Int]()
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
```

## Licence

The MIT License (MIT)

Copyright (c) 2016 by Lloyd Chan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
Status API Training Shop Blog About Pricing
