/* Rank.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Rank {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Rank")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)

	// Load Graph data
	val edges = sqlContext.read.json("/251/graph/language_expertise/*")
	val vertics = sqlContext.read.json("/251/graph/language_expertise_vertics/*")

	// Construct follow graph
	val verticsRdd: RDD[(Long, (String))] = vertics.rdd.map(v => (v(1).toString.toLong, (v(0).toString)))
	val edgesRdd: RDD[Edge[String]] = edges.rdd.map(edge => Edge(edge(4).toString.toLong, edge(2).toString.toLong, edge(0).toString))
	val graph = Graph(verticsRdd, edgesRdd)
	
	// Collect all possible programming languages in the graph
	import sqlContext.implicits._
	val languages = graph.edges.map(e => e.attr).distinct().collect()

	// Run pagerank on a subset graph of each programming language
	for (language <- languages) { val pagerankGraph = graph.subgraph(e => e.attr == language).pageRank(0.001); pagerankGraph.vertices.toDF.repartition(1).write.json("/251/graph/lang-pagerank/" + language)}
  }
}