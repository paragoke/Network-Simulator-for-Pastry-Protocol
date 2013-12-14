import akka.actor.ActorSystem
import akka.actor.Props
import java.security.SecureRandom
import java.util.ArrayList
import java.util.TreeMap
import java.util.Random
import scala.collection.mutable.TreeSet
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import scala.concurrent.duration._
import java.security.MessageDigest
import akka.actor.ActorRef
import akka.actor.Actor
import scala.math._
import java.lang.NullPointerException
import scala.concurrent.duration._

case class START(a : String)
case class initialize_complete(nodeId:String)
case class add_new_node(nodeId:String,node:ActorRef)
case class join_node(nodeId:String,node:ActorRef)
case class join_complete(nodeId:String,node:ActorRef)
case class routing_table_info(routingTable:Array[Array[(String,ActorRef)]])
case class leafset_info(leafset:List[(String,ActorRef)])
case class fail(msg:String)
case class start_Routing(nodeList:List[(String,ActorRef)])
case class start_pastry
case class printroutingtable
case class route(key:String,message:String,hop:Int)
case class route_complete(hops : Int)
case class ini_Join(nId : String,node_ActorRef : ActorRef)
case class join(nid:String,node_ActorRef : ActorRef, hop: Int)
case class join_Complete_Leaf(ls : (String,ActorRef))
case class join_Complete_rTable(rt : (String,ActorRef))
case class share_Leafset(ls : List[(String, ActorRef)],hops: Int)
case class share_Routingtable(mPrefix : Int,rt : Array[(String,ActorRef)])
case class failed(nId : String,node_ActorRef : ActorRef)


object project3 extends App {
  
  val actorSystem = ActorSystem("Application")
  val Application = actorSystem.actorOf(Props(new Application(args(0).toInt,args(1).toInt)),"Application")
  Application ! START("da")
  
}

class Application(numNodes:Int,numReq:Int) extends Actor {
  
  private val actorSystem = ActorSystem("PastryNodes")
  private val noOfNodes:Int = numNodes
  private val noOfRequests:Int = numReq
  private val b = 3
  private val l = 8 
  private val m = 8
  private val base = Math.pow(2, b).toInt
  private val noOfRow = 10//(math.ceil(math.log(noOfNodes)/(b*math.log(2)))).toInt
  private val noofCol = base
  private var activeNodes = 0
  private var readyNodes = 0
  private var joinedNodes = 0
  private var previousHopSum = 0
  private var msgReceivedCount = 0
  private var nodeList: ArrayList[akka.actor.ActorRef] = new ArrayList[akka.actor.ActorRef]()
  private var network: TreeMap[String,akka.actor.ActorRef] = new TreeMap[String,akka.actor.ActorRef]()
  private var failures = 0
  
  /*
   * 
   */
  def createActors(noOfActors:Int,actorSystem:akka.actor.ActorSystem)={
    for(i <- 0 until noOfActors){
      val nodeId = generateNodeId(base)
      val newNode = context.actorOf(Props(new Node(nodeId,noOfNodes,b,l,m,base,noOfRequests)),
          "Node"+(activeNodes).toString)
      nodeList.add(newNode)
      network.put(nodeId, newNode)
      activeNodes = activeNodes + 1
    }
    
  }
  /*
   * 
   */
  def generateNodeId(base:Int):String ={
    
    var randomGenerator:SecureRandom = SecureRandom.getInstance("SHA1PRNG")
    var finished:Boolean = false
    var randomNum = 0
    
    while(!finished){
      randomNum = randomGenerator.nextInt()
      
      if(randomNum>0 && Integer.toString(randomNum,base).length()==10){
        finished=true
      }
    }
   
    Integer.toString(randomNum,base)
  }
  /*
   * 
   */
  def initializeNetwork ={
    
    var nodeIdItr = network.keySet().iterator()
    
    while(nodeIdItr.hasNext()){
      var nodeId = nodeIdItr.next()
      var leafset = generateLeafSet(nodeId)
      var routingTable = generateRoutingTable(nodeId)
      network.get(nodeId) ! routing_table_info(routingTable)
      network.get(nodeId) ! leafset_info(leafset)
     }
   }
  /*
   * 
   */
  def generateLeafSet(nodeId:String):List[(String,ActorRef)] ={
    var actualLeafSet:List[(String,ActorRef)] = List()
    var counter:Int = 1
    
    
    var arr = network.keySet().toArray(Array.ofDim[String]((numNodes/2).ceil.toInt))
    var nodeIndex = arr.indexOf(nodeId)
    
    
    while(actualLeafSet.length<l){
      if((nodeIndex+counter)<=(arr.size).toInt-1&&actualLeafSet.length<l){
    	  actualLeafSet = actualLeafSet :+ (arr(nodeIndex+counter),network.get(arr(nodeIndex+counter)))
    			  counter += 1
      }
      if((nodeIndex-counter)>=0&&actualLeafSet.length<l){
    	  actualLeafSet = actualLeafSet :+ (arr(nodeIndex-counter),network.get(arr(nodeIndex-counter)))
    			  counter += 1
      }
    }
    
    actualLeafSet = sortList(actualLeafSet)
    actualLeafSet
  }
  /*
   * 
   */
  def generateRoutingTable(nodeId:String):Array[Array[(String,ActorRef)]]={
    
    var routingTable:Array[Array[(String,ActorRef)]] = Array.ofDim[(String,ActorRef)](noOfRow,noofCol)
    var shuffnodeIdItr = util.Random.shuffle(network.keySet().toArray(Array.ofDim[String](network.keySet().size())).toList)
    var snodeIdItr = shuffnodeIdItr
    var nodeIdItr = snodeIdItr.iterator 
    var rowNo = 0
    var colNo = 0
    
     while(nodeIdItr.hasNext){
      var currNodeId = nodeIdItr.next()
      
      if(currNodeId==nodeId){
        
      }
      else{
        rowNo = getCommonPrefix(currNodeId,nodeId)
        colNo = currNodeId.charAt(rowNo)-'0'
        
        if(rowNo<noOfRow)
        { 
        	if(colNo==(nodeId.charAt(rowNo)-'0')){
        		routingTable(rowNo)(colNo) = (nodeId,network.get(nodeId))
        	}
        	else{
        		routingTable(rowNo)(colNo) = (currNodeId,network.get(currNodeId))
        	}
        }
        
      }
      
    }
    
 
    routingTable
    
  }
  /*
   * 
   */
  def getCommonPrefix(currNodeId:String,nodeId:String):Int={
    
    var i = 0
    
    while (i < currNodeId.length() && i < nodeId.length() && currNodeId.charAt(i) == nodeId.charAt(i)) {
            i = i + 1
    }
    
    return i
    
  }
  /*
   * 
   */
  def sortedNodes:ArrayList[akka.actor.ActorRef]={
    
    val nodeIterator = network.values().iterator()
    var sorted = new ArrayList[akka.actor.ActorRef]()
    
    while(nodeIterator.hasNext()){
      sorted.add(nodeIterator.next())
    }
    
    sorted
  
  }
  
  def addNewActor():(String,ActorRef)={
    
      val nodeId = generateNodeId(base)
      val newNode = context.actorOf(Props(new Node(nodeId,noOfNodes,b,l,m,base,noOfRequests)),
          "Node"+(activeNodes).toString)
      nodeList.add(newNode)
      network.put(nodeId, newNode)
      activeNodes = activeNodes + 1
      (nodeId,newNode)
    
  }
  def sortList(list: List[(String, ActorRef)]): List[(String, ActorRef)] = {
        var swapped: Boolean = true;
        var nodeIdListArray: Array[(String, ActorRef)] = list.toArray
        var j: Int = 0;
        var tmp: (String, ActorRef) = null
        while (swapped) {
            swapped = false;
            j += 1
            for (i <- 0 until (nodeIdListArray.length - j)) {
                val key1 = Integer.parseInt(nodeIdListArray(i)._1,base)
                val key2 = Integer.parseInt(nodeIdListArray(i + 1)._1,base)
                if (key1 > key2) {
                    tmp = nodeIdListArray(i);
                    nodeIdListArray(i) = nodeIdListArray(i + 1)
                    nodeIdListArray(i + 1) = tmp;
                    swapped = true;
                }
            }
        }
        return (nodeIdListArray.toList)
    }
  
  def receive ={
    
    case route_complete(hops)=>{
      previousHopSum += hops;
      msgReceivedCount += 1;
      val average: Double = previousHopSum.toDouble / msgReceivedCount.toDouble
      println("Hops: "+hops + " no of requests completed: " +msgReceivedCount+ " Average of hops: " + average)
      if (msgReceivedCount == (noOfRequests * noOfNodes)) {
        println("terminated successfully")
        System.exit(0);
      }
      
    }
    
    case join_complete(nodeId,node) =>{

      joinedNodes+=1;
      import context._
      

      if(joinedNodes == (noOfNodes.toFloat/2).floor.toInt){
        
      for(i <- 1 to failures){
        
    	  		var nodeIdList = network.keySet().toArray(Array.ofDim[String](network.keySet().size()))
    	  		
    	  		var random = new Random();
    	  		var randomIndex = random.nextInt(nodeIdList.size)
    	  		network.get(nodeIdList(randomIndex)) ! fail("now")
    	  		nodeList.remove(network.get(nodeIdList(randomIndex)))
    	  		network.remove(nodeIdList(randomIndex))
      		}
    	println("Starting Pastry")
        context.system.scheduler.scheduleOnce(5000 milliseconds)(context.self ! start_pastry)
      }
      
      
    }
    
    case add_new_node(nodeId,node)=>{
      
      var randomGen = new Random();
      var randomIndex = randomGen.nextInt((noOfNodes.toFloat/2).ceil.toInt)
      
      nodeList.get(randomIndex) ! join_node(nodeId,node)
      
    }
    
    case initialize_complete(nodeId)=>{
      
      readyNodes = readyNodes + 1
      if(readyNodes == (noOfNodes.toFloat/2).ceil.toInt){
        println("Starting to add nodes")
        for(i <- 0 until (noOfNodes.toFloat/2).floor.toInt){
          var newNode = addNewActor()
          context.self ! add_new_node(newNode._1,newNode._2)
        }
        
      }
      
    }
    
    case START("da") =>{
      createActors((noOfNodes.toFloat/2).ceil.toInt,actorSystem)
      initializeNetwork
      
    }
    
    case start_pastry =>{
      
      var nodeIdList = network.keySet().toArray(Array.ofDim[String](network.keySet().size()))
      var nodearr = nodeList.toArray(Array.ofDim[ActorRef](nodeList.size()))
      
      for(j <- nodearr){
        
        var nodes: List[(String,ActorRef)] = List()
      	
        for(i <- 1 to noOfRequests){

      				var randomGen = new Random
      				var randomnode = nodeIdList(randomGen.nextInt(nodeIdList.size))
      				nodes = nodes :+ (randomnode,network.get(randomnode))
      	}
      	
      	j ! start_Routing(nodes)
      	
      }
      
    }
    
  }

}

class Node(nid : String, N1 : Int, b1 : Int, l1 : Int, m1 : Int,base:Int, num_Requests : Int) extends Actor {
  
	var N : Int = N1 
	var b : Int = b1
	var l : Int = l1
	var m : Int = m1
	var noOfRequests = num_Requests
	var logbase = base
	var node_Id : String = nid;
	val max_Pastry_Count = num_Requests
	var routing_Table_Count = 0
	var ls_Received = false
	var rt_Received = false
	var node_ActorRef = null 
	var Application = context.parent
	var leaf_Set : List[(String,ActorRef)] = List()
	val row : Int = 10
	val col : Int = (math.pow(2, b)).toInt
	var routing_Table : Array[Array[(String,ActorRef)]] = Array.ofDim[(String,ActorRef)](row,col) //
	var network = Array[ActorRef]()
	var random = new Random()
	var msg_Key : String =""
	var index : Int = 0
	var routingTableReceivedCount =0
	var presentinnetwork:List[String] = List()
	
	
	
	
	def receive = {
	  
	case failed(nId : String,node_ActorRef : ActorRef) =>{
	  if(leaf_Set.contains(nId)){
	    var arr = leaf_Set.toArray
	    var index = arr.indexOf(nId)
	    arr(index) = null
	    leaf_Set = arr.toList
	  }
	  
	  for(i <- 0 until row)
	    for(j<- 0 until col){
	      if(routing_Table(i)(j) != null){
	        if(routing_Table(i)(j)._1 == nId)
	          routing_Table(i)(j) = null
	      }
	    }
	
	}
	
	case fail(msg:String) => {
	  for(i<- leaf_Set){
	    i._2 ! failed(node_Id,context.self)
	  }
	  println(context.self.path.name+"failed with node id "+ node_Id)
	  context.stop(context.self)
	}
	  
	case routing_table_info(rt : Array[Array[(String,ActorRef)]]) =>{
		routing_Table = rt
		rt_Received = true
		if(ls_Received == true) sender ! initialize_complete(node_Id)
	}
	
	case leafset_info(ls : List[(String,ActorRef)]) =>{
		leaf_Set = ls
		ls_Received = true
		if(rt_Received == true) sender ! initialize_complete(node_Id)
	}
	
	
	case join_node(newNodeId : String, node_ActorRef : ActorRef) =>{
		context.self ! join(newNodeId,node_ActorRef,0)
	}
	case join(nId:String, node_ActorRef : ActorRef, hop: Int) =>{
	  
	  var t =  next_Node(nId)
	  
	  if(t==node_Id){
	    node_ActorRef ! share_Leafset(sortList(leaf_Set :+ (node_Id,context.self)),hop)
	  }
	  
	  else if( Math.abs(Integer.parseInt(t._1,base)-Integer.parseInt(nId,base)) 
	      < Math.abs(Integer.parseInt(node_Id,base)-Integer.parseInt(nId,base))){

	    
	    t._2 ! join(nId,node_ActorRef,hop+1)
	    
	    var matchedPrefix = shl(node_Id,nId)
	    
	    if(matchedPrefix<row){
	    	node_ActorRef ! share_Routingtable(matchedPrefix,routing_Table(matchedPrefix))
	  	}
	  
	  }
	  
	  else{ 
	    node_ActorRef ! share_Leafset(sortList(leaf_Set :+ (node_Id,context.self)),hop)
	  }
	}  
	
	case join_Complete_Leaf(ls : (String,ActorRef) ) => {
	  if(!leaf_Set.contains(ls)){
	    if(!presentinnetwork.contains(ls._1))
	      presentinnetwork = presentinnetwork :+ ls._1  

	    var arr = Array.ofDim[(String, akka.actor.ActorRef)](leaf_Set.length)
	    leaf_Set.copyToArray(arr)
	    
	    if((Integer.parseInt(ls._1,base)-Integer.parseInt(node_Id,base))<0){
	      arr(0) = ls
	    }
	    
	    else{
	      arr(l-1) = ls
	    }
	    
	    leaf_Set = arr.toList
	    leaf_Set = sortList(leaf_Set)
	  
		}
	}
	
	case join_Complete_rTable(rt : (String,ActorRef)) => {
			var t3 = shl(rt._1, node_Id)
			if(t3<row){
			  
			  val colNum = rt._1.charAt(t3) - '0'
			  
			  if (routing_Table(t3)(colNum) == null) { // adding new node's entry to
				  routing_Table(t3)(colNum) = rt 
				  if(!presentinnetwork.contains(rt._1))
			    		presentinnetwork = presentinnetwork :+ rt._1  
			  }
			
			}
	}
	
	
	case route(key:String, message:String, hop : Int) =>{
	  msg_Key = key
	  if(msg_Key==node_Id){
		
	    Application ! route_complete(hop+1)
	  
	  }
	  else{
	    
	    var t = next_Node(msg_Key)
	    if(t._1==node_Id){
	      Application ! route_complete(hop+1)
	    }
	    else{
	     // println(node_Id+" forwarded key "+msg_Key+" to "+t._1)
	      t._2 ! route(msg_Key,message,hop+1)

	    }

	  
	  }
	}
	
	case share_Leafset(ls : List[(String, ActorRef)], hops: Int)=>{
	  leaf_Set = ls.dropRight(1)
	  
	  ls_Received = true
	  import context.dispatcher
	  var stopScheduler = false
	  var executed = false
	  val cancellable = context.system.scheduler.schedule(0 milliseconds,1 milliseconds) {
                        if (routingTableReceivedCount == hops&&(!executed)) {
                            Application ! join_complete(node_Id,node_ActorRef)
                            stopScheduler = true
                            send_Own_State()
                            executed = true
                            
                        }
                    }
	   context.system.scheduler.schedule(0 milliseconds,1 milliseconds) {
                        if (stopScheduler == true) {
                           cancellable.cancel
                            
                        }
                    }
	}
	
	case share_Routingtable(mPrefix : Int, rt : Array[(String,ActorRef)])=>{ // on the route of JOIN every node sends its rtable(somerow) to destination
	  routingTableReceivedCount +=1
	  var currRow = mPrefix
	  var j = 0
	  /*
	   * get one node
	   */
	  for(i <- rt){
	     
	    if(i==null){
	      routing_Table(currRow)(j) = i
	    }
	    
	    else if(shl(node_Id,i._1)!=mPrefix){
	        var r = shl(node_Id,i._1)
	        var c = i._1.charAt(r) -'0'
	        if(r<row){
	        if(routing_Table(r)(c) == null)
	          routing_Table(r)(c) = i
	        }
	    }
	   
	    else if(routing_Table(currRow)(j)==null){
	      routing_Table(currRow)(j) = i
	    }
	    
	    j+=1
	  }
	}
	case start_Routing(list : List[(String,ActorRef)]) =>{
		for(i <- list){
			context.self ! route(i._1,"Hello",0)	
		
		}
	
	}
	
	case printroutingtable =>{
	  printRoutingTable
	}
	
	
	
	}
	
	def shl(nId : String, key:String) : Int = {
	  var i : Int = 0
	  while (i < nId.length() && i < key.length() && nId.charAt(i) == key.charAt(i)) {
            i += 1;
        }
        i
	}
	
	
	def send_Own_State(){
	  leaf_Set.foreach(
            leaf =>{
                if (leaf != null) {
                	val t0 = (node_Id,context.self)
                    leaf._2 ! join_Complete_Leaf(t0);
                }
            })

        for (i <- 0 until row) {
            for (j <- 0 until col) {
                if (routing_Table != null && routing_Table(i)(j) != null) {
                	val t0 = (node_Id,context.self)
                    routing_Table(i)(j)._2 ! join_Complete_rTable(t0)
                }
            }
        }

	}
	

	
	def check_In_Leafset(key: String) : (String,ActorRef) ={

	  val keyToSearchBase10 = Integer.parseInt(key,base)
	  val maxLeaf = Integer.parseInt(leaf_Set( leaf_Set.length - 1)._1,base)
      val minLeaf = Integer.parseInt(leaf_Set(0)._1,base)
      
      if(keyToSearchBase10<minLeaf||keyToSearchBase10>maxLeaf)
        return null
      
      var minDiff = Math.abs(keyToSearchBase10 - Integer.parseInt(node_Id,base))
      var minLeafNode:(String,ActorRef) = (node_Id,context.self)      
      val prefixnode = shl(node_Id,key)
        
      for(i <- leaf_Set){
        
        var prefixi = shl(i._1,key)
        
        if(Math.abs(keyToSearchBase10 - Integer.parseInt(i._1,base))< minDiff){
          minDiff = Math.abs(keyToSearchBase10 - Integer.parseInt(i._1,base))
          minLeafNode = i
        }
        
      }
	  
	  return minLeafNode
      
	}
	
	
	def check_In_Routingtable(key:String) : (String, ActorRef)= {
	    var next_Node : (String,ActorRef) = null
		var t1 = shl(node_Id,key)
	    
		if(t1>(row-1))	
		  return next_Node
	    
		val c: Int = key.charAt(t1) - '0';

	    if (routing_Table != null && routing_Table(t1)(c) != null) {
            next_Node = routing_Table(t1)(c)

        }
	    
	    next_Node
	    
	}
	
	def check_Rest(key : String) : (String,ActorRef) = { 
	  val prefixMatchedWithCurrentNode = shl(node_Id,key)
      val keyBase10 = Integer.parseInt(key,base)
	  val distanceFromCurrentNode = Math.abs(Integer.parseInt(node_Id,base) - keyBase10)
	  var list:List[(String,ActorRef)] = List()
	  
	  for(i <- leaf_Set){
	  
	    if(i!=null)
	    	list = list :+ i
	  
	  }

      for(i <- 0 until row) {
            for (j <- 0 until col) {
            
              if(routing_Table(i)(j)!=null&&(node_Id.charAt(i)-'0')!=j)
            	  list = list :+ routing_Table(i)(j)
           
            }
      }
	  
	  var minDiff = Math.abs(keyBase10 - Integer.parseInt(node_Id,base))
	  var minLeafNode:(String,ActorRef) = (node_Id,context.self)
	  
	  for(i <- list.iterator){
	    
	    val prefixwithi = shl(i._1,key)
        
        if(Math.abs(keyBase10 - Integer.parseInt(i._1,base))< minDiff&&prefixMatchedWithCurrentNode<=prefixwithi){
          minDiff = Math.abs(keyBase10 - Integer.parseInt(i._1,base))
          minLeafNode = i
        }
        
      }
	  
	  return minLeafNode
	}
	
	def next_Node(key : String) : (String,ActorRef)= {
	  var index : (String,ActorRef) = null

	  
	  index = check_In_Leafset(key)
	  if(index!=null){ 
	    	return index 
	  }
	  
	  index = check_In_Routingtable(key)
	  if(index!= null) {
	    	return index 
	    	}
	  
	  index = check_Rest(key)  
	  
	  index
	  
	}
	
	def printRoutingTable():String={
	  
	  var rt =""
	 
	  println("Routing Table for "+node_Id)
	  for(i <- 0 until row){
	    for(j <- 0 until col){
	      if(routing_Table(i)(j)==null){
	    	  rt = rt + "null "
	      }
	      else{
	    	 rt = rt + routing_Table(i)(j)._1+" "
	      }
	    }
	  	rt = rt + "\n"
	  	}
	  	rt
	}
	
	def sortList(list: List[(String, ActorRef)]): List[(String, ActorRef)] = {
        var swapped: Boolean = true;
        var nodeIdListArray: Array[(String, ActorRef)] = list.toArray
        var j: Int = 0;
        var tmp: (String, ActorRef) = null
        while (swapped) {
            swapped = false;
            j += 1
            for (i <- 0 until (nodeIdListArray.length - j)) {
                val key1 = Integer.parseInt(nodeIdListArray(i)._1,base)
                val key2 = Integer.parseInt(nodeIdListArray(i + 1)._1,base)
                if (key1 > key2) {
                    tmp = nodeIdListArray(i);
                    nodeIdListArray(i) = nodeIdListArray(i + 1)
                    nodeIdListArray(i + 1) = tmp;
                    swapped = true;
                }
            }
        }
        return (nodeIdListArray.toList)
    }
}