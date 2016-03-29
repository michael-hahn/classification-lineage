/**
 * Created by Michael on 11/13/15.
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





class Test extends userTest[(String, Int)] with Serializable {

  def usrTest(inputRDD: RDD[(String, Int)], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    /*the rest of the code are for correctness test
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD, maxClusters, totalClusters, centroid_ref)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/Classification_LineageDD/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification.jar", "org.apache.hadoop.examples.Classification", "-m", "3", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[Int, List[Long]] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/Classification_LineageDD/output/part-00000").getLines()) {
      val token = new StringTokenizer(line.trim)
      val key  = token.nextToken().toInt
      var value = token.nextToken().toLong
      if (truthList.contains(key)) {
        var newList = value::truthList(key)
        truthList = truthList updated (key, newList)
      } else {
        truthList = truthList updated (key, List(value))
      }
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (!truthList.contains(tupVal._1)) {
        returnValue = true
        println("TruthList does not contain " + tupVal._1)
      }
      else {
        val itr2 = tupVal._2.toIterator
        while (itr2.hasNext) {
          val itrVal = itr2.next().toLong
          if (!truthList(tupVal._1).contains(itrVal)){
            returnValue = true
            //println("TruthList with key " + tupVal._1 + " has the value: " + truthList(tupVal._1)
             //+ " and it does not contain " + itrVal)
          } else {
            val updateList = truthList(tupVal._1).filter(_ != itrVal)
            truthList = truthList updated (tupVal._1, updateList)
            //println("Remove " + itrVal + " from key" + tupVal._1)
          }
        }
        if (!truthList(tupVal._1).isEmpty) {
          returnValue = true
          //println("TruthList with key " + tupVal._1 + " is not empty")
        } else {
          truthList = truthList - tupVal._1
        }
      }
    }
    if (!truthList.isEmpty) {
      returnValue = true
      //println("TruthList is not empty")
    }

    val outputFile = new File("/Users/Michael/IdeaProjects/Classification_LineageDD/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    */
    inputRDD.collect().foreach(println)
    val finalRdd = DeltaWorkflowManager.generateNewWorkFlow(inputRDD)
    val out = finalRdd.collect()
    for (o <- out) {
      println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    return returnValue
  }
}
