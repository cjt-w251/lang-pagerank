/* PrepareGraph.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PrepareGraph {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("PrepareGraph")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)

	// Generate Graph Edges
	val df = sqlContext.read.json("/251/data/2015/*/*/*")
	val watchEvents = df.filter(df("type") === "WatchEvent")
	val watchTuples = watchEvents.select(watchEvents("actor")("id"), watchEvents("actor")("login"), watchEvents("repo")("name")).withColumnRenamed("actor[login]", "watcher").withColumnRenamed("actor[id]", "watcherId").withColumnRenamed("repo[name]", "watchedRepo")

	val pushEvents = df.filter(df("type") === "PushEvent")
	val pushTuples = pushEvents.select(pushEvents("actor")("id"), pushEvents("actor")("login"), pushEvents("repo")("name")).withColumnRenamed("actor[login]", "pusher").withColumnRenamed("actor[id]", "pusherId").withColumnRenamed("repo[name]", "pushedRepo")

	val followTuples = watchTuples.join(pushTuples, watchTuples("watchedRepo") === pushTuples("pushedRepo")).select("watcher", "watcherId", "pusher", "pusherId", "watchedRepo")
	
	val reposRdd = sc.textFile("/251/repos/cut_repos.csv")

	import sqlContext.implicits._
	import au.com.bytecode.opencsv.CSVParser
	val reposWithLang = reposRdd.filter(repo => {val parser = new CSVParser(','); parser.parseLine(repo)(2) != ""}).map(repo => {val parser = new CSVParser(','); val parsed = parser.parseLine(repo); (parsed(0), parsed(2))}).toDF

	val followGraph = followTuples.join(reposWithLang, followTuples("watchedRepo") === reposWithLang("_1")).withColumnRenamed("_2","language").select("watcher","watcherId","pusher","pusherId","language").dropDuplicates()
	followGraph.write.json("/251/graph/language_expertise")

	// Generate Graph Vertics
	//val edges = sqlContext.read.json("/251/graph/language_expertise/*")
	//val pushers = edges.select("pusherId","pusher")
	//val watchers = edges.select("watcherId","watcher")
	//val followees = pushers.unionAll(watchers).dropDuplicates()
	//followees.repartition(1).write.json("/251/graph/language_expertise_vertics")
  }
}