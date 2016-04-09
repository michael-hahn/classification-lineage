/**
 * Created by Michael on 4/5/16.
 */

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
object Classification_noLineage {
  private val strModelFile = "/Users/Michael/IdeaProjects/Classification/initial_centroids.data"
  private val maxClusters = 16
  private val centroids = new Array[Cluster](maxClusters)
  private val centroids_ref = new Array[Cluster](maxClusters)

  //  def mapFunc(str: String): (Int, String) = {
  //    val token = new StringTokenizer(str)
  //    val key = token.nextToken().toInt
  //    var value = token.nextToken()
  //    (key, value)
  //  }


  //    def testGroundTruthFunc[K:ClassTag,V:ClassTag](outputRDD: RDD[(Int, String)],path:String,f: String => (K , V) ): List[Int] ={
  //      val rdd = outputRDD.sparkContext.textFile(path)
  //      val transformed:PairRDDFunctions[K,V] = rdd.map(f)
  //      val output = outputRDD.collect()
  //      var joinedRDD = transformed.join(outputRDD.asInstanceOf[RDD[(K,V)]])
  //      val filter = joinedRDD.filter(r =>r._2._1 != r._2._2)
  //      val failures = filter.collect()
  //      var index = 0
  //      var list = List[Int]()
  //      for(o <- output){
  //        if(o.isInstanceOf[Tuple2[K,V]]){
  //          val tmp = o.asInstanceOf[Tuple2[K,V]]
  //          for(f <- failures) {
  //            if (f._1 == tmp._1) {
  //              list = index :: list
  //            }
  //          }
  //        }
  //        index = index + 1
  //      }
  //      return list
  //
  //  }

  def initializeCentroids(): Int = {
    var numClust = 0
    for (i <- 0 until maxClusters) {
      centroids(i) = new Cluster()
      centroids_ref(i) = new Cluster()
    }
    val modelFile = new File(strModelFile)
    val opnScanner = new Scanner(modelFile)
    while (opnScanner.hasNext) {
      val k = opnScanner.nextInt()
      centroids_ref(k).similarity = opnScanner.nextFloat()
      centroids_ref(k).movie_id = opnScanner.nextLong()
      centroids_ref(k).total = opnScanner.nextShort()
      val reviews = opnScanner.next()
      val revScanner = new Scanner(reviews).useDelimiter(",")
      while (revScanner.hasNext) {
        val singleRv = revScanner.next()
        val index = singleRv.indexOf("_")
        val reviewer = new String(singleRv.substring(0, index))
        val rating = new String(singleRv.substring(index + 1))
        val rv = new Review()
        rv.rater_id = reviewer.toInt
        rv.rating = rating.toInt.toByte
        centroids_ref(k).reviews.add(rv)
      }
    }
    for (pass <- 1 until maxClusters) {
      for (u <- 0 until (maxClusters - pass)) {
        if (centroids_ref(u).movie_id < centroids_ref(u+1).movie_id) {
          val temp = new Cluster(centroids_ref(u))
          centroids_ref(u) = centroids_ref(u+1)
          centroids_ref(u+1) = temp
        }
      }
    }
    for (l <- 0 until maxClusters) {
      if (centroids_ref(l).movie_id != -1) {
        numClust = numClust + 1
      }
    }
    numClust
  }

  //  def main(args:Array[String]): Unit = {
  //    val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/Classification_LineageDD/lineageResult"))
  //
  //    val sparkConf = new SparkConf().setMaster("local[8]")
  //    sparkConf.setAppName("Classification_LineageDD-" )
  //      .set("spark.executor.memory", "2g")
  //
  //    val ctx = new SparkContext(sparkConf)
  //
  //    val lines = ctx.textFile("/Users/Michael/IdeaProjects/Classification/file1_dbug", 1)
  //    val totalClusters = initializeCentroids()
  //    val constr = new sparkOperations
  //    val output = constr.sparkWorks(lines, maxClusters, totalClusters, centroids_ref).collect
  //    val itr = output.iterator
  //    while (itr.hasNext) {
  //      val tupVal = itr.next()
  ////      pw.append(tupVal._1 + " " + tupVal._2 + "\n")
  //      pw.append(tupVal._1 + ":")
  //      val itr2 = tupVal._2.toIterator
  //      while (itr2.hasNext) {
  //        val itrVal = itr2.next()
  //        pw.append(itrVal + "\n")
  //      }
  //
  //    }
  //    pw.close()
  //    ctx.stop()
  //  }


  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[6]")
      sparkConf.setAppName("Classification_LineageDD")
        .set("spark.executor.memory", "2g")


      //set up lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }
      //

      //set up spark context
      val ctx = new SparkContext(sparkConf)



      //generate truth file (for correctness test only)
      /*
            //Prepare for Hadoop MapReduce
            val clw = new commandLineOperations()
            clw.commandLineWorks()
            //Run Hadoop to have a groundTruth
            Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification.jar", "org.apache.hadoop.examples.Classification", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/Classification/file1s", "output").!!
      */
      //generate centroid
      val totalClusters = initializeCentroids()

      //start counting the time for the lineage to finish
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //run spark with lineage
      val lines = ctx.textFile("/Users/Michael/IdeaProjects/Classification/file1s.data", 1)

      //      for (o <- output) {
      //        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      //      }

      //      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/Classification_LineageDD/lineageResult"))


      //print out the resulting list for debugging purposes
      //      for (l <- list) {
      //        println("*************************")
      //        println(l)
      //        println("*************************")
      //      }



      val mappedRDD = lines
        .filter(line => {
          val movieIndex = line.indexOf(":")
          if (movieIndex > 0) true
          else false
        })
        .map(s => {
        val index = s.indexOf(":")
        val key = s.substring(0, index)
        val value = s.substring(index + 1)
        (key, value)
      })

      mappedRDD.cache()
      //      println("MappedRDD has " + mappedRDD.count() + " records")


      //      pw.close()

      //      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Classification_LineageDD/lineageResult", 1)
      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Classification/file1s", 1)

      //      val num = lineageResult.count()
      //      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/Classification_LineageDD/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
      //lineageResult.cache()

      //      if (exhaustive == 1) {
      //        val delta_debug: DD[String] = new DD[String]
      //        delta_debug.ddgen(lineageResult, new Test,
      //          new Split, maxClusters, totalClusters, centroids_ref, lm, fh)
      //      } else {
      val delta_debug = new DD_NonEx_v2[(String, String)]
      val returnedRDD = delta_debug.ddgen(mappedRDD, new Test_v2, new Split_v2, maxClusters, totalClusters, centroids_ref, lm, fh)
      //      }

      val ss = returnedRDD.collect.foreach(println)



      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " microseconds")

      //total time
      logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (DeltaDebuggingEndTime - LineageStartTime)/1000 + " microseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }

      println("Job's DONE!")
      ctx.stop()
    }
  }
}
