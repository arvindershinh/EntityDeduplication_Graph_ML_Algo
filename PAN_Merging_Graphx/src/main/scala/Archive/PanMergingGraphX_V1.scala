package Archive

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PanMergingGraphX_V1 extends App  {

  val conf = new SparkConf().setAppName("PAN Merging").setMaster("local")
  val sc = new SparkContext(conf)

  val nodes: RDD[(VertexId, String)] =
    sc.parallelize(Seq(
      (1L, "N1"), (2L, "N2"), (3L, "N3"), (4L, "N4"), (5L, "N5"), (6L, "N6"), (7L, "N7"), (8L, "N8")
      )
    )

  val edges: RDD[Edge[String]] =
    sc.parallelize(Seq
    (Edge(1L, 2L, "1->2"),Edge(2L, 3L, "2->3"),Edge(1L, 3L, "1->3"),Edge(2L, 4L, "2->4"),
      Edge(6L, 5L, "6->5"),Edge(7L, 5L, "7->5"),Edge(8L, 5L, "8->5"))
    )

  val graph: Graph[String, String] = Graph(nodes, edges)

  def Merging(graph: Graph[String, String]): List[RDD[(VertexId, (String, Int))]] = {

    def MergingRecursion(graph: Graph[String, String], clsID: Int):List[RDD[(VertexId, (String, Int))]] ={

      if (graph.vertices.count() == 0) {
        List()
      } else {

        val ID = graph.vertices.collect.head._1

        val initialGraph = graph.mapVertices((id, vd) =>
          if (id == ID) (vd, clsID) else (vd, 0))

        val pregelGraph: Graph[(String, Int), String] = initialGraph.pregel(0, Int.MaxValue, EdgeDirection.Either)(
          (id, attr, msgClsID) => if (msgClsID == 0) attr else (attr._1, msgClsID), // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr._2 != 0 & triplet.dstAttr._2 == 0) {
              Iterator((triplet.dstId, triplet.srcAttr._2))
            }
            else if (triplet.srcAttr._2 == 0 & triplet.dstAttr._2 != 0) {
              Iterator((triplet.srcId, triplet.dstAttr._2))
            } else {
              Iterator.empty
            }
          },
          (a, b) => if (a == 0) b else a
        )
        val residueGraph = pregelGraph.subgraph(vpred = (id, attr) => attr._2 == 0).mapVertices((id, vd) => vd._1)

        pregelGraph.subgraph(vpred = (id, attr) => attr._2 == clsID).vertices :: MergingRecursion(residueGraph, clsID + 1)
      }
    }

    MergingRecursion(graph, 1)
  }

  val clusters = Merging(graph)

  clusters.foreach(vertexCluster => vertexCluster.foreach(v => println(v._1+"-->"+v._2._1+"->"+v._2._2)))

}
