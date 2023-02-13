package Archive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object SparkGraphX_Learning2 extends App {
  val conf = new SparkConf().setAppName("PAN Merging").setMaster("local")
  val sc = new SparkContext(conf)

  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  // Define a default user in case there are relationship with missing user
  val defaultUser = ("John Doe", "Missing")
  // Build the initial Graph
  val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

  println(graph.vertices.filter { case (id, (name, pos)) => pos == "prof" }.count)

}
