package com.beachape.sparkka

import akka.actor._
import _root_.akka.stream.scaladsl.Flow
import akka.pattern._
import akka.util.Timeout
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.{ ActorSupervisorStrategy, ActorHelper }

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Created by Lloyd on 2/28/16.
 */
object Streaming {

  /**
   * Returns an InputDStream of type FlowElementType along with a Flow map element that you can use to attach to
   * your flow.
   *
   * <em>NOTE</em> ensure that you have remoting set up properly in your application.conf; see http://doc.akka.io/docs/akka/snapshot/scala/remoting.html
   *
   * <em>Backpressures</em> if and when the initial buffer is filled and your input stream has not yet started; in this event, the
   * buffer is held constant up until the provided initial buffer wait duration, after which point we start dropping oldest buffer
   * entries, and backpressure stops.
   *
   * @param actorName Name of the receiver actor (default: "akka-stream-receiver-$uuid")
   * @param initialBufferSize In the event that the InputStream is not yet started, how many elements from the Akka stream
   *                          should be buffered before dropping oldest entries (default: 50000)
   * @param initialBufferWait In the event that the InputStream is not yet started and the buffer is full, how long to "block"
   *                          for until we begin culling oldest items from the buffer (default: 15 seconds)
   * @param storageLevel RDD storage level (default: StorageLevel.MEMORY_AND_DISK_SER_2) for the receiver
   * @param supervisorStrategy Supervisor strategy for the receiver actor (default: ActorSupervisorStrategy.defaultStrategy)
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
  def connection[FlowElementType: ClassTag](
    actorName: String = randomUniqueName("akka-stream-receiver"),
    initialBufferSize: Int = 50000,
    initialBufferWait: FiniteDuration = 15.seconds,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
    supervisorStrategy: SupervisorStrategy = ActorSupervisorStrategy.defaultStrategy
  )(implicit actorSystem: ActorSystem, streamingContext: StreamingContext): (ReceiverInputDStream[FlowElementType], Flow[FlowElementType, FlowElementType, Unit]) = {
    val feederActor = actorSystem.actorOf(Props(new FlowShimFeeder[FlowElementType](initialBufferSize, initialBufferWait)))
    val feederActorPath = absoluteAddress(feederActor.path)
    val inputDStreamFromActor = streamingContext.actorStream[FlowElementType](
      props = Props(new FlowShimReceiver(feederActorPath)),
      name = actorName,
      storageLevel = storageLevel,
      supervisorStrategy = supervisorStrategy
    )
    implicit val askTimeout = Timeout(initialBufferWait + 500.millis)
    import actorSystem.dispatcher
    val flow = Flow[FlowElementType].mapAsync(1 /* Otherwise, the feeding actor gets elements out of order */ ) { p =>
      (feederActor ? p).map(_ => p)
    }
    (inputDStreamFromActor, flow)
  }

  private def absoluteAddress(path: ActorPath)(implicit actorSystem: ActorSystem): String = {
    val remoteAddress = RemoteAddressExtension(actorSystem).address
    path.toStringWithAddress(remoteAddress)
  }

  private def randomUniqueName(nameBase: String): String = {
    val uuid = java.util.UUID.randomUUID.toString
    s"$nameBase-$uuid"
  }

  // Seems rather daft to need 2 actors to do this, but otherwise we run into serialisation problems with the Akka Stream

  private sealed case class Subscribe(ref: ActorRef)
  private sealed case class UnSubscribe(ref: ActorRef)
  private case object Ok

  private class FlowShimFeeder[FlowElementType: ClassTag](initialBufferSize: Int, bufferWaitTime: Duration) extends Actor with ActorLogging {
    import context.become

    def receive = initialState()

    /*
     * Trivial initial state
     */
    private def initialState(): Receive = {
      case element: FlowElementType => {
        // on receiving an element, enter the state where we buffer until there is a subscriber
        sender ! Ok
        become(bufferUntilSubscribed(Queue(element)))
      }
      case Subscribe(ref) => become(subscribed(Seq(ref)))
      case other => log.error(s"Received an unexpected message: $other")
    }

    /*
     * main buffering state, with a special case for when when we are about to hit the buffer limit
     */
    private def bufferUntilSubscribed(toSend: Queue[FlowElementType]): Receive = {
      case d: FlowElementType if toSend.size == (initialBufferSize - 1) => {
        // if we are just one short of the maximum buffer size, get ready to go into the state where we hold the buffer constant until we run out of time
        val waitUntil = System.currentTimeMillis() + bufferWaitTime.toMillis
        become(constantBufferUntilSubscribed(sender, manageBuffer(toSend, d), waitUntil))
        sender ! Ok // This is the last acknowledgement before we get subscribers or start culling
      }
      case d: FlowElementType => {
        become(bufferUntilSubscribed(manageBuffer(toSend, d)))
        sender ! Ok
      }
      case Subscribe(ref) => {
        // Send everything in the buffer to the subscriber, then enter the subscribed state
        toSend.foreach(ref ! _)
        become(subscribed(Seq(ref)))
      }
      case other => log.error(s"Received an unexpected message: $other")
    }

    /*
     * In this state, we hold a reference to the sender of the last piece of data and hold the buffer constant by sending
     * any new data elements to self over and over again until we can wait no longer
     */
    private def constantBufferUntilSubscribed(originalSender: ActorRef, toSend: Queue[FlowElementType], waitUntil: Long): Receive = {
      case d: FlowElementType if System.currentTimeMillis() <= waitUntil => {
        // hold the buffer by sending the element to self without replying to the original sender so we won't get more data
        self ! d
      }
      case d: FlowElementType if System.currentTimeMillis() > waitUntil => {
        // holding for too long now; go back to the mode where we buffer until there is a subscriber
        become(bufferUntilSubscribed(manageBuffer(toSend, d)))
        originalSender ! Ok // Reply back to the original sender so we can get more data
      }
      case Subscribe(ref) => {
        toSend.foreach(ref ! _)
        become(subscribed(Seq(ref)))
        originalSender ! Ok // Let the original sender things are good and to send more data, since a subscriber has arrived
      }
      case other => log.error(s"Received an unexpected message: $other")
    }

    private def subscribed(subscribers: Seq[ActorRef]): Receive = {
      case d: FlowElementType => {
        subscribers.foreach(_ ! d)
        sender ! Ok
      }
      case Subscribe(ref) => become(subscribed(ref +: subscribers))
      case UnSubscribe(ref) => become(subscribed(subscribers.filterNot(_ == ref)))
      case other => log.error(s"Received an unexpected message: $other")
    }

    private def manageBuffer(existingBuffer: Queue[FlowElementType], d: FlowElementType): Queue[FlowElementType] = {
      if (existingBuffer.size >= initialBufferSize) {
        log.warning(s"${self.path}'s buffer is full (max $initialBufferSize) but there are still no subscribers, dropping oldest entries.")
      }
      existingBuffer.takeRight(initialBufferSize - 1) :+ d
    }
  }

  private class FlowShimReceiver[FlowElementType: ClassTag](feederAbsoluteAddress: String) extends Actor with ActorHelper {
    private lazy val feederActor = context.system.actorSelection(feederAbsoluteAddress)
    override def preStart(): Unit = feederActor ! Subscribe(self)
    override def postStop(): Unit = feederActor ! UnSubscribe(self)

    def receive = {
      case p: FlowElementType => store(p)
      case other => log.error(s"Received a random message: $other")
    }
  }

}