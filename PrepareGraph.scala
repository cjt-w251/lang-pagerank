/* PrepareGraph.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PrepareGraph {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("PrepareGraph")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)

	// Generate Graph Edges ( Note we are only using 2015 event data because pre-2015, event schema is not consistent )
	val df = sqlContext.read.json("/251/data/2015/*/*/*")

	// Get all the watch events
	val watchEvents = df.filter(df("type") === "WatchEvent")
	val watchTuples = watchEvents.select(watchEvents("actor")("id"), watchEvents("actor")("login"), watchEvents("repo")("name")).withColumnRenamed("actor[login]", "watcher").withColumnRenamed("actor[id]", "watcherId").withColumnRenamed("repo[name]", "watchedRepo")

	// Get all the commmit(push) events
	val pushEvents = df.filter(df("type") === "PushEvent")
	val pushTuples = pushEvents.select(pushEvents("actor")("id"), pushEvents("actor")("login"), pushEvents("repo")("name")).withColumnRenamed("actor[login]", "pusher").withColumnRenamed("actor[id]", "pusherId").withColumnRenamed("repo[name]", "pushedRepo")

	// Infer a follow relationship by UserA watches RepoX, UserB commited to RepoX => UserA follows UserB
	val followTuples = watchTuples.join(pushTuples, watchTuples("watchedRepo") === pushTuples("pushedRepo")).select("watcher", "watcherId", "pusher", "pusherId", "watchedRepo")
	
	// Load all the repo data
	val reposRdd = sc.textFile("/251/repos/cut_repos.csv")
	import sqlContext.implicits._
	import au.com.bytecode.opencsv.CSVParser
	val reposWithLang = reposRdd.filter(repo => {val parser = new CSVParser(','); parser.parseLine(repo)(2) != ""}).map(repo => {val parser = new CSVParser(','); val parsed = parser.parseLine(repo); (parsed(0), parsed(2))}).toDF

	// Join the follow relationship with repo data to infer programming language as a property of the follow relationship
	val followGraph = followTuples.join(reposWithLang, followTuples("watchedRepo") === reposWithLang("_1")).withColumnRenamed("_2","language").select("watcher","watcherId","pusher","pusherId","language").dropDuplicates()
	followGraph.write.json("/251/graph/language_expertise")

	// Generate Graph Vertics 
	val pushers = followGraph.select("pusherId","pusher")
	val watchers = followGraph.select("watcherId","watcher")
	val followees = pushers.unionAll(watchers).dropDuplicates()
	followees.repartition(1).write.json("/251/graph/language_expertise_vertics")
  }
}