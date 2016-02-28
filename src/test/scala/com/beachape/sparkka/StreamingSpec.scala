package com.beachape.sparkka

import akka.actor.ActorSystem
import akka.stream.{ SourceShape, ActorMaterializer }
import akka.stream.scaladsl._
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.{ Matchers, FunSpec }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }

/**
 * Created by Lloyd on 2/28/16.
 */
class StreamingSpec extends FunSpec with ScalaFutures with Matchers with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(150, Millis))

  describe("streamConnection") {

    it("should allow both the original flow and the connected InputDStream to receive all expected values") {
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
    }

  }

}