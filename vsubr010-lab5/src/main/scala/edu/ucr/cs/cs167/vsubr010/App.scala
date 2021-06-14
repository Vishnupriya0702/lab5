package edu.ucr.cs.cs167.vsubr010

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args : Array[String]) {
    val command: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    conf.setAppName("lab5")
    val sparkContext = new SparkContext(conf)
    try {
      val inputRDD: RDD[String] = sparkContext.textFile(inputfile)
      // TODO Parse the input file using the tab separator and skip the first line
      val filterheader : String =inputRDD.first()
      val finalRDD : RDD[String] = inputRDD.filter( x=> x!=filterheader)
      // Parsing the data
      val parsedata : RDD[Array[String]] = finalRDD.map(x=>x.split('\t'))

      val t1 = System.nanoTime
      command match {
        //case "count-all" =>

        //TODO count total number of records in the file
          //val totalcount: Long = parsedata.count()
         // println(s"Total count for file '${command}' is '${totalcount}'")
       //case "code-filter" =>
         //TODO Filter the file by response code, args(2), and print the total number of matching lines
         // val filtercode : String = args(2)
         //val responsecode : RDD[String] =finalRDD.map(x=>x.split('\t')(5))
         //val filterfile : RDD[String] = responsecode.filter(x=> x==filtercode)
         //val countbyfilter : Long = filterfile.count()
      //println(s"Total count for file '${inputfile}'  with response code '${filtercode}' is '${countbyfilter}'")
        //case "time-filter" =>
         //TODO Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
          //val fromdate : String = args(2)
          //val Todate : String =args(3)
        //val Datatofilter : RDD[String] =finalRDD.map(x=>x.split('\t')(2))
        // val finaloutput : RDD[String] =Datatofilter.filter(x=>(x>=fromdate) && (x<=Todate))
          //val countbytimefilter : Long = finaloutput.count()
         //println(s"Total count for file '${inputfile}' in time range '${fromdate}' and '${Todate}' is '${countbytimefilter}'")

       //case "count-by-code" =>
         //TODO Group the lines by response code and count the number of records per group
       // val groupdata  = finalRDD.map(x=>(x.split('\t')(5),1))
        //  val countdata  =groupdata.countByKey()
         //countdata.foreach(x=>(println(x)))

        //case "sum-bytes-by-code" =>
         //TODO Group the lines by response code and sum the total bytes per group
       //val groupdata  = finalRDD.map(x=>(x.split('\t')(5),x.split('\t')(6) ))
        //val sumdata  =groupdata.reduceByKey((x,y) => x+y)
        //val collectdata =sumdata.collect()
       //collectdata.foreach(x=>(println(x)))
       case "avg-bytes-by-code" =>
        // TODO Group the liens by response code and calculate the average bytes per group
        //val groupdata  = finalRDD.map(x=>(x.split('\t')(5),1))
        //val countdata  =groupdata.countByKey()
        //val sumdata  = finalRDD.map(x=>(x.split('\t')(5),x.split('\t')(6) ))
         //val groupdata  = finalRDD.map(x=>(x.split('\t')(5),1))
        //val sumdata1  =sumdata.reduceByKey((x,y) => x+y)/groupdata.countBykey()
         // val avgdata = sumdata1.map(x=>x_.1)/
          //sumdata1.foreach(x=>(println(x)))

          val groupdata  = finalRDD.map(x=>(x.split('\t')(5).toInt,x.split('\t')(6).toInt ))
          val aggregateddata = groupdata.aggregateByKey((0, 0))((sum, value)=>(sum._1+value, sum._1+1) ,(total1, total2)=>(total1._1+total1._2, total2._1+ total2._2))
         val collectdata= aggregateddata.collect()
      println(s"Total count for file '${inputfile}' is '${collectdata}''")
        //case "top-host" =>
         //TODO print the host the largest number of lines and print the number of lines
       val hostdata  = finalRDD.map(x=>(x.split('\t')(0),1))
        //val sumdata  =hostdata.reduceByKey((x,y) => x+y)
          //val interchangedata = sumdata.map(_.swap)
         //val sortdata = interchangedata.sortByKey(false)
         //val finaldata = sortdata.first()
          //println(s"largest number of lines '${finaldata}' ")

       //case "comparison" =>
         //TODO Given a specific time, calculate the number of lines per response code for the
        //val fromdate : String = args(2)
         //val Todate : String =args(3)
          //val Datatofilter : RDD[String] =finalRDD.map(x=>x.split('\t')(2))
          //val filterbefore = Datatofilter.filter(x=>(x<=fromdate))
          //val filterAfter = Datatofilter.filter(x=>(x>=fromdate))
          //val groupdatabefore  = filterbefore.map(x=>(x.split('\t')(5),1))
          //val countdatabefore  =groupdatabefore.countByKey()
          //val groupdataafter  = filterAfter.map(x=>(x.split('\t')(5),1))
          //val countdataafter  =groupdataafter.countByKey()
         //countdatabefore.foreach(x=>(println(x)))
         // countdataafter.foreach(x=>(println(x)))
      }
     val t2 = System.nanoTime
      println(s"Command '${command}' on file '${inputfile}' finished in ${(t2-t1)*1E-9} seconds")
    } finally {
     sparkContext.stop
   }
 }
}