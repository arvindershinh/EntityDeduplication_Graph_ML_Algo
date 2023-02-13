package Archive

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkGraphX_Learning3 extends App {

  val conf = new SparkConf().setAppName("PAN Merging").setMaster("local")
  val sc = new SparkContext(conf)

  val nodes: RDD[(VertexId, String)] =
    sc.parallelize(Seq(
      (1L, "N1"), (2L, "N2"), (3L, "N3"), (4L, "N4"), (5L, "N5"), (6L, "N6"), (7L, "N7"), (8L, "N8")
      )
    )

  val nodes_join: RDD[(VertexId, String)] =
    sc.parallelize(Seq(
      (3L, "NJ3"), (4L, "NJ4"), (5L, "NJ5")
    )
    )

  val edges: RDD[Edge[String]] =
    sc.parallelize(Seq
      (Edge(1L, 2L, "1->2"),Edge(2L, 3L, "2->3"),Edge(1L, 3L, "1->3"),Edge(2L, 4L, "2->4"),
        Edge(6L, 5L, "6->5"),Edge(7L, 5L, "7->5"),Edge(8L, 5L, "8->5"))
    )

//  val edges: RDD[Edge[String]] =
//    sc.parallelize(Seq
//    (Edge(1L, 2L, "edge"),Edge(2L, 3L, "edge"),Edge(3L, 4L, "edge"),Edge(4L, 5L, "edge"),
//      Edge(5L, 6L, "edge"),Edge(6L, 7L, "edge"),Edge(7L, 8L, "edge"))
//    )

  val graph: Graph[String, String] = Graph(nodes, edges)

//  #######################################################################

//  graph.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach(e => println(e.attr))
//  graph.outDegrees.foreach(v => println(v._1+"#->"+v._2))

//  val graph1 = graph.joinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.toString)
  //  graph1.vertices.foreach(v => println(v._1+"$->"+v._2))

//  val graph2 = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
  //  graph2.vertices.foreach(v => println(v._1+"$->"+v._2))

//  val graph3 = graph.joinVertices(nodes_join)((vid, _, newAttr) => newAttr)
//    graph3.vertices.foreach(v => println(v._1+"$->"+v._2))

//  val graph4 = graph.outerJoinVertices(nodes_join)((vid, attr, newAttr) => newAttr.getOrElse(attr.reverse))
//  graph4.vertices.foreach(v => println(v._1+"$->"+v._2))

//  graph.vertices.foreach(v => println(v._1))

  val ID = graph.vertices.collect.head._1

  println(ID)

  //  #######################################################################

  val clsID1: Int = 1
  val ID1 = 4


  val initialGraph = graph.mapVertices((id, vd) =>
    if (id == ID1) (vd, clsID1) else (vd, 0))

//  initialGraph.vertices.foreach(v => println(v._1+" "+v._2._1+" "+v._2._2))

  val pregelGraph = initialGraph.pregel(0, Int.MaxValue, EdgeDirection.Either)(
    (id, attr, msgClsID) => if (msgClsID == 0) attr else (attr._1, msgClsID), // Vertex Program
    triplet => {  // Send Message
      if (triplet.srcAttr._2 != 0 & triplet.dstAttr._2 == 0) {
        Iterator((triplet.dstId, triplet.srcAttr._2))
      }
      else if (triplet.srcAttr._2 == 0 & triplet.dstAttr._2 != 0) {
        Iterator((triplet.srcId, triplet.dstAttr._2))
      } else {Iterator.empty}
    },
    (a, b) => if (a == 0) b else a
  )

  pregelGraph.vertices.foreach(v => println(v._1+"-->"+v._2._1+"->"+v._2._2))


//  -----------------------------------------

  val subGraph1 = pregelGraph.subgraph(vpred = (id, attr) => attr._2 == 1).mapVertices((id, vd) => vd._1)
//    subGraph1.vertices.foreach(v => println(v._2))

  val residueGraph = pregelGraph.subgraph(vpred = (id, attr) => attr._2 == 0).mapVertices((id, vd) => vd._1)
//    residueGraph.vertices.foreach(v => println(v._2))
//    residueGraph.edges.foreach(e => println(e.attr))


  val clsID2: Int = 2
  val ID2 = 5

  val initialResidueGraph = residueGraph.mapVertices((id, vd) =>
    if (id == ID2) (vd, clsID2) else (vd, 0))

  val pregelResidueGraph = initialResidueGraph.pregel(0, Int.MaxValue, EdgeDirection.Either)(
    (id, attr, msgClsID) => if (msgClsID == 0) attr else (attr._1, msgClsID), // Vertex Program
    triplet => {  // Send Message
      if (triplet.srcAttr._2 != 0 & triplet.dstAttr._2 == 0) {
        Iterator((triplet.dstId, triplet.srcAttr._2))
      }
      else if (triplet.srcAttr._2 == 0 & triplet.dstAttr._2 != 0) {
        Iterator((triplet.srcId, triplet.dstAttr._2))
      } else {Iterator.empty}
    },
    (a, b) => if (a == 0) b else a
  )

  pregelResidueGraph.vertices.foreach(v => println(v._1+"-->"+v._2._1+"->"+v._2._2))

}
