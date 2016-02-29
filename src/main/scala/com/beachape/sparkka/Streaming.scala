package com.beachape.sparkka

import akka.actor._
import _root_.akka.stream.scaladsl.Flow
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.immutable.Queue
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
   * @param actorName Name of the receiver actor
   * @param flowBufferSize In the event that the InputStream is not yet ready, how many elements from the Akka stream
   *                       should be buffered before dropping oldest entries
   * @param actorSystem
   * @param streamingContext
   * @tparam FlowElementType
   * @example
   // format: OFF
   * {{{
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
   * }}}
   //  format: ON
   */
  def streamConnection[FlowElementType: ClassTag](actorName: String = randomUniqueName("akka-stream-receiver"), flowBufferSize: Int = 5000)(implicit actorSystem: ActorSystem, streamingContext: StreamingContext): (ReceiverInputDStream[FlowElementType], Flow[FlowElementType, FlowElementType, Unit]) = {
    val feederActor = actorSystem.actorOf(Props(new FlowShimFeeder[FlowElementType](flowBufferSize)))
    val feederActorPath = absoluteAddress(feederActor.path)
    val inputDStreamFromActor = streamingContext.actorStream[FlowElementType](Props(new FlowShimPublisher(feederActorPath)), actorName)
    val flow = Flow[FlowElementType].map { p =>
      feederActor ! p
      p
    }
    (inputDStreamFromActor, flow)
  }

  private def absoluteAddress(path: ActorPath)(implicit actorSystem: ActorSystem): String = {
    val remoteAddress = RemoteAddressExtension(actorSystem).address
    path.toStringWithAddress(remoteAddress)
  }

  // Seems rather daft to need 2 actors to do this, but otherwise we run into serialisation problems with the Akka Stream
  private class FlowShimFeeder[FlowElementType: ClassTag](flowBufferSize: Int) extends Actor with ActorLogging {
    import context.become
    def receive = awaitingSubscriber(Queue.empty)
    def awaitingSubscriber(toSend: Queue[FlowElementType]): Receive = {
      case d: FlowElementType => {
        if (toSend.size >= flowBufferSize) {
          log.warning(s"${self.path}'s buffer is full (max $flowBufferSize) but there are still no subscribers, dropping oldest entries ")
        }
        val newToSendQueue = toSend.takeRight(flowBufferSize) :+ d
        become(awaitingSubscriber(newToSendQueue))
      }
      case Subscribe(ref) => {
        log.debug(s"Got a subscriber (${ref.path}), emptying buffer")
        toSend.foreach(ref ! _)
        become(subscribed(Seq(ref)))
      }
      case other => log.error(s"Received a random message: $other")
    }

    def subscribed(subscribers: Seq[ActorRef]): Receive = {
      case p: FlowElementType => subscribers.foreach(_ ! p)
      case Subscribe(ref) => become(subscribed(ref +: subscribers))
      case UnSubscribe(ref) => become(subscribed(subscribers.filterNot(_ == ref)))
      case other => log.error(s"Received a random message: $other")
    }
  }
  private class FlowShimPublisher[FlowElementType: ClassTag](feederAbsoluteAddress: String) extends Actor with ActorHelper {
    private lazy val feederActor = context.system.actorSelection(feederAbsoluteAddress)
    override def preStart(): Unit = feederActor ! Subscribe(self)
    override def postStop(): Unit = feederActor ! UnSubscribe(self)

    def receive = {
      case p: FlowElementType => store(p)
      case other => log.error(s"Received a random message: $other")
    }
  }

  private sealed case class Subscribe(ref: ActorRef)
  private sealed case class UnSubscribe(ref: ActorRef)

  private def randomUniqueName(nameBase: String): String = s"$nameBase-${uuid()}"

  private def uuid() = java.util.UUID.randomUUID.toString
}