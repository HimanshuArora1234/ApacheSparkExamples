package fr.him

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToAsyncRDDActions

import scala.collection.Map

/**
 * JOB to find the location where the max number of each product has been sold.
 */
class SalesJob2(sc: SparkContext) {
  // reads data from text files and computes the results. This is what you test
  def run(t: String, u: String) : RDD[(String, String)] = {
    val transactions = sc.textFile(t)
    val newTransactionsPair = transactions.map{t =>
      val p = t.split("\t")
      (p(2).toInt, p(4))
    }

    val users = sc.textFile(u)
    val newUsersPair = users.map{t =>
      val p = t.split("\t")
      (p(0).toInt, p(3))
    }

    val result = processData(newTransactionsPair, newUsersPair)
    return sc.parallelize(result.toSeq).map(t => (t._1, t._2))
  }

  def processData (t: RDD[(Int, String)], u: RDD[(Int, String)]) : Array[(String,String)] = {
    var jn = t.join(u).values
    jn.groupByKey().map[(String, String)](x => (x._1, x._2.groupBy(identity).mapValues(_.size).maxBy(_._2)._1)).collect()
  }
}

object SalesJob2 {
  def main(args: Array[String]) {
    val transactionsIn = "transactions_test.txt"
    val usersIn = "user_test.txt"
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    val context = new SparkContext(conf)
    val job = new SalesJob2(context)
    val results = job.run(transactionsIn, usersIn)
    val output = "output2"
    results.saveAsTextFile(output)
    context.stop()
  }
}