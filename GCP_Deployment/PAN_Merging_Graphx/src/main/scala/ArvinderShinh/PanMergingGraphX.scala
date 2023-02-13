package ArvinderShinh

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3
import java.io.File


object PanMergingGraphX  {

  val spark = SparkSession.builder
    .appName("PAN Merging")
    .master("local[1]")
    .getOrCreate

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val input_URL = getClass.getClassLoader.getResource("PAN_Groups_Sample.csv")

    val file = spark.sparkContext.textFile(input_URL.toString);

    val nodes: RDD[(VertexId, String)] = file.flatMap(line => line.split(",")).distinct().map(s => (MurmurHash3.stringHash(s), s))

    val edges: RDD[Edge[String]] = file.map(line => line.split(","))
      .map(line => Edge(MurmurHash3.stringHash(line(0)), MurmurHash3.stringHash(line(1)), "PAN has GroupID"))

    val graph: Graph[String, String] = Graph(nodes, edges)

    graph.triplets.foreach(e => println(e.srcAttr+" "+e.srcId+" "+e.attr+" "+e.dstId+" "+e.dstAttr))

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

    val cls_DF = clusters.flatMap(vertexCluster => vertexCluster.collect).map(v => (v._1.toString, v._2._1, v._2._2))
      .toDF("NodeID", "Node", "ClusterID")

    val output_path = input_URL.toString.split("/").filter(p => p != "PAN_Groups_Sample.csv").reduce((agg, str) => agg+"/"+str)+"\\PAN_Merging0.csv"

    cls_DF.coalesce(1).write.csv(output_path)

    print("PAN Merging Job successfully completed - Arvinder Shinh")

//  print(input_URL.toString.split("/").filter(p => p != "PAN_Groups_Sample.csv").reduce((agg,str) => agg+"/"+str)+"\\PAN_Merging0.csv")

  }

}

//CACHE

//private val output_URL = getClass.getClassLoader.getResource()
//val file = new File(input_URL.toURI)
//private val parent = file.getParent
//private val output_path = parent + "\\PAN_Merging0.csv"
