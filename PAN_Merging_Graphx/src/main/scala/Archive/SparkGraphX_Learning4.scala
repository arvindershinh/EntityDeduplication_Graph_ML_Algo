package Archive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3

object SparkGraphX_Learning4  extends App  {
  //create SparkContext
  val sparkConf = new SparkConf().setAppName("GraphFromFile").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  // read your file
  /*suppose your data is like
  v1 v3
  v2 v1
  v3 v4
  v4 v2
  v5 v3
  */
  val file = sc.textFile("D:/Workspace/Spark/PAN_Merging/PAN_Merging_Graphx/src/main/resources/archive/TestGraph.csv");

  val nodes: RDD[(VertexId, String)] = file.flatMap(line => line.split(",")).distinct().map(s => (MurmurHash3.stringHash(s), s))
//  nodes.foreach(v => println(v._1+" "+v._2))

  val edges: RDD[Edge[String]] = file.map(line => line.split(","))
    .map(line => Edge(MurmurHash3.stringHash(line(0)), MurmurHash3.stringHash(line(1)), "PAN has GroupID"))
//  edges.foreach(e => println(e.srcId+" "+e.attr+" "+e.dstId))

  val graph: Graph[String, String] = Graph(nodes, edges)

  graph.triplets.foreach(e => println(e.srcAttr+" "+e.srcId+" "+e.attr+" "+e.dstId+" "+e.dstAttr))



//  // create a graph
  //  val graph = Graph.fromEdgeTuples(edgesRDD, 1)
  //
  //  // you can see your graph
  //  graph.triplets.collect.foreach(println)

//  edgeTripletRDD.foreach(t => println(t._1.toString()+" "+t._2.toString()+" "+t._3.toString))


}
