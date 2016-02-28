package com.beachape.sparkka

import _root_.akka.actor.Actor
import _root_.akka.actor.ActorLogging
import _root_.akka.actor.ActorRef
import _root_.akka.actor.ActorSystem
import _root_.akka.actor.Props
import _root_.akka.stream.scaladsl.Flow
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.ActorHelper

import scala.reflect.ClassTag

/**
 * Created by Lloyd on 2/28/16.
 */
object Streaming {

  /**
   * Returns an InputDStream of type FlowElementType along with a Flow map element that you can use to attach to
   * your flow.
   *
   * Requires your system to have proper Akka remote configurations set up.
   *
   * @param flowBufferSize In the event that the InputStream is not yet ready, how many elements from the Akka stream
   *                       should be buffered before dropping oldest entries
   * @param actorSystem
   * @param streamingContext
   * @tparam FlowElementType
   * @example
   *
   * Format: OFF
   * {{{
   * import akka.actor.ActorSystem
   * import akka.stream.{ActorMaterializer, ClosedShape}
   * import akka.stream.javadsl.{Sink, RunnableGraph}
   * import akka.stream.scaladsl._
   * import com.beachape.sparkka._
   * implicit val actSys = ActorSystem()
   * implicit val materializer = ActorMaterializer()
   *
   * val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
   *
   * import GraphDSL.Implicits._
   *
   * val source = Source(1 to 10)
   *
   * val sink = builder.add(Sink.ignore)
   * val bCast = builder.add(Broadcast[Int](2))
   * val merge = builder.add(Merge[Int](2))
   *
   * // InputDStream can then be used to build elements of the graph that require integration with Spark
   * val (inputDStream, feedDInput) = SparkIntegration.streamConnection[Int]()
   *
   * val add1 = Flow[Int].map(_ + 1)
   * val times3 = Flow[Int].map(_ * 3)
   * source ~> bCast ~> add1 ~> merge ~> sink
   * bCast ~> times3 ~> feedDInput ~> merge
   *
   * ClosedShape
   * })
   * }}}
   * Format: ON
   */
  def streamConnection[FlowElementType: ClassTag](flowBufferSize: Int = 5000)(implicit actorSystem: ActorSystem, streamingContext: StreamingContext): (ReceiverInputDStream[FlowElementType], Flow[FlowElementType, FlowElementType, Unit]) = {
    val feederActor = actorSystem.actorOf(Props(new FlowShimFeeder[FlowElementType](flowBufferSize)))
    val remoteAddress = RemoteAddressExtension(actorSystem).address
    val feederActorPath = feederActor.path.toStringWithAddress(remoteAddress)
    val inputDStreamFromActor = streamingContext.actorStream[FlowElementType](Props(new FlowShimPublisher(feederActorPath)), "smiling-classifier-training-stream")
    val flow = Flow[FlowElementType].map { p =>
      feederActor ! p
      p
    }
    (inputDStreamFromActor, flow)
  }

  // Seems rather daft to need 2 actors to do this, but otherwise we run into serialisation problems with the Akka Stream
  private class FlowShimFeeder[FlowElementType: ClassTag](flowBufferSize: Int) extends Actor with ActorLogging {
    def receive = awaitingSubscriber(Nil)
    def awaitingSubscriber(toSend: Seq[FlowElementType]): Receive = {
      case d: FlowElementType => context.become(awaitingSubscriber(toSend.takeRight(flowBufferSize) :+ d))
      case ref: ActorRef => {
        toSend.foreach(ref ! _)
        context.become(subscribed(ref))
      }
      case other => log.error(s"Received a random message: $other")
    }

    def subscribed(subscriber: ActorRef): Receive = {
      case p: FlowElementType => subscriber ! p
      case other => log.error(s"Received a random message: $other")
    }
  }
  private class FlowShimPublisher[FlowElementType: ClassTag](feederAbsoluteAddress: String) extends Actor with ActorHelper {
    private val feederActor = context.system.actorSelection(feederAbsoluteAddress)
    override def preStart(): Unit = feederActor ! self

    def receive = {
      case p: FlowElementType => store(p)
      case other => log.error(s"Received a random message: $other")
    }
  }

}