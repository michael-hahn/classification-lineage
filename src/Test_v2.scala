/**
 * Created by Michael on 4/5/16.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.delta.DeltaWorkflowManager
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.{Scanner, Calendar, StringTokenizer}

import scala.collection.mutable.MutableList
import scala.io.Source
import scala.reflect.ClassTag

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import org.apache.spark.SparkContext._
import scala.sys.process._

class Test_v2 extends userTest[(String, String)] with Serializable {
  def usrTest(inputRDD: RDD[(String, String)], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    //this is for the version without goNext
    val resultRDD = inputRDD
    .map(line => {
          var movieIdStr = new String()
          var reviewStr = new String()
          var userIdStr = new String()
          var reviews = new String()
          var tok = new String("")

          var clusterId = 0
          val n = new Array[Int](maxClusters)
          val sq_a = new Array[Float](maxClusters)
          val sq_b = new Array[Float](maxClusters)
          val numer = new Array[Float](maxClusters)
          val denom = new Array[Float](maxClusters)

          var max_similarity = 0.0f
          var similarity = 0.0f
          val movie = new Cluster()

          for (r <- 0 until maxClusters) {
            numer(r) = 0.0f
            denom(r) = 0.0f
            sq_a(r) = 0.0f
            sq_b(r) = 0.0f
            n(r) = 0
          }
          //MovieIndex is guaranteed to be larger than 0 due to the first filter operation
          movieIdStr = line._1
          val movieId = movieIdStr.toLong
          movie.movie_id = movieId
          reviews = line._2
          val token = new StringTokenizer(reviews, ",")

          while(token.hasMoreTokens){
            tok = token.nextToken()
            val reviewIndex = tok.indexOf("_")
            userIdStr = tok.substring(0, reviewIndex)
            reviewStr = tok.substring(reviewIndex + 1)
            val userId = userIdStr.toInt
            val review = reviewStr.toInt
            for (r <- 0 until totalClusters) {
              breakable {
                for (q <- 0 until centroid_ref(r).total) {
                  val rater = centroid_ref(r).reviews.get(q).rater_id
                  val rating = centroid_ref(r).reviews.get(q).rating.toInt
                  if (userId == rater) {
                    numer(r) += (review * rating).toFloat
                    sq_a(r) += (review * review).toFloat
                    sq_b(r) += (rating * rating).toFloat
                    n(r) += 1
                    break
                  }
                }
              }
            }
          }
          for (p <- 0 until totalClusters) {
            denom(p) = ((Math.sqrt(sq_a(p).toDouble)) * (Math.sqrt(sq_b(p).toDouble))).toFloat
            if (denom(p) > 0) {
              similarity = numer(p) /denom(p)
              if (similarity > max_similarity) {
                max_similarity = similarity
                clusterId = p
              }
            }
          }
          (clusterId, movieIdStr)
        })
      .groupByKey()
      //this final stage throws an exception
        .map(s => {
          var value = new String("")
          val itr = s._2.toIterator
          while (itr.hasNext) {
            value += itr.next().asInstanceOf[(String, Long)]._1
            value += ","
          }
          value = value.substring(0, value.length - 1)
          val lla = value.split(",")
          if (lla.contains("13")) value += "*"
//          println("***********" + value+  "!!!!!!!!!!!!!!!!!!!")
          (s._1, value)
        })

    val out = resultRDD.collect()

    for (o <- out) {
      //      println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    return returnValue
  }
}
