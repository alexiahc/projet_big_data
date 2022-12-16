package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            .appName("Big Data Project")
            .enableHiveSupport()
            .getOrCreate()

        // For implicit conversions
        import spark.implicits._

        var data = spark.read.option("header", true).csv("E:/upm/BigData/1987.csv")
        // preprocessing 
        data = data.drop("ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
        data.show()
    }
}
