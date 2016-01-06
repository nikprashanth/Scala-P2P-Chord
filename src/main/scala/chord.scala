
import akka.actor._
import akka.pattern.ask
import scala.collection.mutable.ListBuffer
import java.util.Arrays.ArrayList
import scala.math._
import akka.util.Timeout
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

case class initFingerTable(p: ActorRef)
case class updateFingertable(s: ActorRef, i: Int)

case class findSuccessor(id: Int)
case class vertexJoin(p: ActorRef)
case class getNormalSuccessor()
case class getPredecessor(nRef: ActorRef)
case class getNormalPredecessor()
case class setNormalPredecessor(predecesssor: ActorRef)

case class updateOthers()

case class notifyNode(nRef: ActorRef)
case class isLive()
case class killNode()

/**
 * @author imstellar
 */
object chord {

  def main(args: Array[String]) {

    var numNodes: Int = args(0).toInt

    var numRequests: Int = args(1).toInt

    if (args.length != 2) {
      println("valid # of args is -> program numNodes numRequests ")
      System.exit(1)
    }
    println(" ----simulation of chord P2P using scala-akka framework-----")

    val actorSystem = ActorSystem("ChordP2P")
    val vertexHandler = actorSystem.actorOf(Props(new VertexHandler(numNodes, numRequests)), name = "vertexhandler")

    vertexHandler ! "startP2P"
  }
}

class Finger(var start: Int, var node: ActorRef, var succ: ActorRef) {

  def getStart(): Int = {
    this.start
  }
  def setStart(s: Int) = {
    this.start = s
  }
  def setNode(s: ActorRef) = {
    this.node = s
  }
  def getNode(): ActorRef = {
    this.node
  }

}
// finger table list of arrays
/*
 * assumption m =3, we have nodes only starting from 0 till 7
 */
class Vertex(m: Int, r: Int) extends Actor with ActorLogging {
  var n: Int = (self.path.name).toInt
  var nRef: ActorRef = null
  var size: Int = (math.pow(2, m)).toInt
  var successor: ActorRef = self
  var predecessor: ActorRef = null
  implicit val t_out = Timeout(10000000 seconds)
  var fingerTable: Array[Finger] = new Array[Finger](m)
  var alive: Boolean = true
  var hops: Int = 0
  // check periodically
  //context.system.scheduler.schedule(0 milliseconds,50 milliseconds,self,fixFingers())
  //context.system.scheduler.schedule(0 milliseconds,50 milliseconds,self,stabilize())

  def receive = {
    //current instance joins the network
    //p is an arbitrary node in the network
    case vertexJoin(nRef) => {
      //log.info("fingertable size="+fingerTable.length)
      populateFingers()
      for (i <- 0 to m - 1) {
        log.info(fingerTable(i).start.toString)
      }
      log.info("vertexJoin " + n)
      if (nRef != null) {
        
        this.predecessor = null
        var future = nRef ? findSuccessor(n)
        var result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
        this.successor = result

        initFingerTable(nRef)
        updateOthers()
        context.system.scheduler.schedule(0 milliseconds,5000 milliseconds,self,checkPredecessor()) 
      } else {
        for (i <- 0 to m - 1) {
          fingerTable(i).setNode(self)
        }
        this.predecessor = self
      }
    }
    case updateFingertable(sRef: ActorRef, i: Int) => {

      var z = ((fingerTable(i).getNode()).path.name).toInt
      var s = (sRef.path.name).toInt
      //if (n <= s && s < z) {
      if (belongs(true, n, z, false, s)) {
        fingerTable(i).setNode(sRef) //node of s
        predecessor ! updateFingertable(sRef, i)
      }
    }
    case findSuccessor(id: Int) => {

      var nRef: ActorRef = findPredecessor(id)

      var future = nRef ? getNormalSuccessor()
      var result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]

      log.info("average no. of hops ="(hops / r).toString)
      sender ! result
    }
    case getNormalPredecessor() => {

      sender ! this.predecessor
    }
    case setNormalPredecessor(predecessor: ActorRef) => {

      this.predecessor = predecessor
    }
    case getNormalSuccessor() => {

      sender ! this.successor
    }
    case notifyNode(nRef: ActorRef) => {
      var nRefVal = (nRef.path.name).toInt
      var predecessorVal = (predecessor.path.name).toInt
      //if(predecessor ==null || (predecessorVal< nRefVal && nRefVal<n)){ //interval open braces
      if (predecessor == null || belongs(false, predecessorVal, n, false, nRefVal)) {
        this.predecessor = nRef
      }
    }
    case isLive() => {
      log.info("islive")
      sender ! alive
    }
    case killNode() => { //removing disconnect and modifying deactivate to kill
      alive = false
      context.stop(self)
    }
    case _ =>
      log.info("default case called!!! ")
  }

  /*
   * definition starts
   */

  def populateFingers() = {
    var total: Int = (math.pow(2, m)).toInt
    for (i <- 0 to m - 1) {
      fingerTable(i) = new Finger((n + (math.pow(2, i)).toInt) % total, null, null)
    }

  }
  def initFingerTable(nRef: ActorRef) = {
    var future = nRef ? findSuccessor(fingerTable(0).getStart())
    var result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
    fingerTable(0).setNode(result)

    future = successor ? getNormalPredecessor()
    result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
    successor ! setNormalPredecessor(self)

    for (i <- 0 to m - 2) {
      var d = fingerTable(i + 1).getStart()
      var y = ((fingerTable(i).getNode()).path.name).toInt
      //if (n <= d && d < h) {
      if (belongs(true, n, y, false, d)) {
        fingerTable(i + 1).setNode(fingerTable(i).getNode())
      } else {
        future = nRef ? findSuccessor(d)
        result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
        fingerTable(i + 1).setNode(result)
      }
    }
  }

  def updateOthers() = {
    for (i <- 0 to m - 1) {
      // ith finger might be n
      var nRef = findPredecessor(n - (math.pow(2, i - 1)).toInt)
      nRef ! updateFingertable(self, i)
    }
  }
  def stabilize() = {
    var future = successor ? getNormalPredecessor()
    var result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
    var x = (result.path.name).toInt
    var succ = (successor.path.name).toInt
    //if(n<x && x<succ){            // interval open braces
    if (belongs(false, n, succ, false, x)) {
      this.successor = result
    }

    successor ! notifyNode(self)
  }
  def fixFingers() = {
    var random = new Random
    var i = random.nextInt(m)
    var future = nRef ? findSuccessor(fingerTable(i).getStart())
    var result = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
    fingerTable(i).setNode(result)
    //fingerTable(i).setNode(findSuccessor(fingerTable(i+1).getStart))
  }
  def checkPredecessor() = {
    var future = predecessor ? isLive()
    var result = Await.result(future, t_out.duration).asInstanceOf[Boolean]
    if (result == false) {
      this.predecessor = null
    }
  }

  def findPredecessor(id: Int): ActorRef = {

    var nRef: ActorRef = self //initially self but later it gets changed
    var x = (self.path.name).toInt
    var y = (successor.path.name).toInt

    //while (!(x<id && id<=y)) { //!Belongs(id, nRef, nRef.node ! getSuccessor() )){
    while (!belongs(false, x, y, true, id)) {

      nRef = closestPreceedingFinger(nRef,id)

      x = (nRef.path.name).toInt
      //implicit val t_out = Timeout(5 seconds)
      var future = nRef ? getNormalSuccessor()
      nRef = Await.result(future, t_out.duration).asInstanceOf[ActorRef]
      y = (nRef.path.name).toInt

      hops = hops + 1
    }
    nRef
  }
      
    def closestPreceedingFinger(n:ActorRef,id:Int):ActorRef = {

      var done = false
     // implicit val tieout = Timeout(500 seconds)
      var fingerTabls:Array[Finger]=null
      var future = n ? getFingersTable()
      println("hkj"+ future)
      var fingerTables = Await.result(future, 1000000 second).asInstanceOf[Array[Finger]]
      println("lplplp")
      println(fingerTables)
      for (i <- m-1 to 0 by -1) {
        var x = ((fingerTables(i).getNode()).path.name).toInt
        //if (i < x && x < id) { //fingerTable(i).node belongs (n,id)
        if(belongs(false,i,id,false,x)){
          done = true
          return fingerTables(i).getNode()
        }
      }
      return n
    }
    
     def getFingersTable(): Array[Finger] = {
      this.fingerTable
      
    }
  def belongs(left: Boolean, start: Int, end: Int, right: Boolean, id: Int): Boolean = {
    var x = start
    var y = end
    var z = id
    if (x > y) {
      y = y + size
      z = z + size
    }
    if (left && right) {
      if (z <= x && x <= y) {
        true
      }
    }
    if (!left && right) {
      if (z < x && x <= y) {
        true
      }
    }
    if (left && !right) {
      if (z <= x && x < y) {
        true
      }
    }
    if (!left && !right) {
      if (z < x && x < y) {
        true
      }
    }
    false
  }
}
class VertexHandler(n: Int, r: Int) extends Actor with ActorLogging {
  /*
 * Idea here is that we are considering a network of size, that could fit 'n' no. of nodes
 * suppose if n=5, we are creating a network of size 8(i., forming 2 power m, m=3 here)
 * we are not using any hash function, to uniquely identify the node. 
 * we are generating sequence of unique actors starting from 0 to numNodes-1
 */
  val m = ceil(log10(n) / log10(2)).toInt
  var vertexList: ListBuffer[ActorRef] = new ListBuffer()
  var activeNodeList: ListBuffer[ActorRef] = new ListBuffer()

  def receive = {
    case "startP2P" => {
      for (i <- 0 until n) {
        val vertex = context.actorOf(Props(new Vertex(m, r)), name = i.toString)
        vertexList.append(vertex)
      }
      // node0 joins
      vertexList(0) ! vertexJoin(null)
      activeNodeList.append(vertexList(0))
      //var j:Int=0; // randomly generate j
      for (i <- 0 until n - 1) {
        context.system.scheduler.scheduleOnce(5 * (i + 1) seconds) {
          /*
         * join node i using arbitrary node out of already available nodes in the network
         */
          var vRef: ActorRef = activeNodeList(Random.nextInt(activeNodeList.length)) //randomly choose active node
          vertexList(i + 1) ! vertexJoin(vRef) // pick a node 
          activeNodeList.append(vRef) // update active node list
          //vertexList(i+1) ! vertexJoin(vertexList(0)) //node1 is joined with the help of vertex0
        }

        var id: Int = 0
        vertexList(0) ! findSuccessor(id)
      }
    }
    case _ =>
      log.info("default case called!!!")
  }
}