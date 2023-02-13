package Archive

import org.apache.spark.{SparkConf, SparkContext}

object SparkGraphX_Learning1 extends App {
  val conf = new SparkConf().setAppName("PAN Merging").setMaster("local")
  val sc = new SparkContext(conf)

  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)

  print(distData.collect().toList)
}
