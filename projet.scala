
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("My first Spark application")
        val sc = new SparkContext(conf)

        //val data = sc.textFile("file:///tmp/book/98.txt")
        val data = sc.textFile("hdfs:///tmp/book/98.txt")
        val numAs = data.filter(line => line.contains("a")).count()
        val numBs = data.filter(line => line.contains("b")).count()
        println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
    }
    
    def delete_cols(data : DataFrame) {
        cols = [ArrTime, ActualElapsedTime, AirTime, TaxiIn, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay]
        for (col in cols){
            data supp col 
        }
    }
}


