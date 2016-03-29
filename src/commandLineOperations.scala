/**
 * Created by Michael on 11/12/15.
 */

import scala.sys.process._

class commandLineOperations {
  def commandLineWorks(): Unit = {
    val compile = Seq("javac", "-cp", "/Users/Michael/Downloads/hadoop-2.6.2/etc/hadoop:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/common/lib/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/common/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/hdfs:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/hdfs/lib/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/hdfs/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/yarn/lib/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/yarn/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/mapreduce/lib/*:/Users/Michael/Downloads/hadoop-2.6.2/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar", "-d", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification_Java/Classification.java", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification_Java/Cluster.java", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification_Java/Review.java").!!
    //println(compile)
    val makeJar = Seq("jar", "-cvf", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification.jar", "-C", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Classification/", ".").!!
    //val hadoopOp = Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/HistogramMovies.jar", "org.apache.hadoop.examples.HistogramMovies", filePath, "output").!!
  }
}
