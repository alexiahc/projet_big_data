package upm.bd
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)

        // config de l'app a changer apres 

        //val conf = new SparkConf().setAppName("Big Data Project")
        //val sc = new SparkContext(conf)

        val spark = SparkSession
            .builder()
            .appName("Big Data Project")
            .enableHiveSupport()
            .getOrCreate()   
        import spark.implicits._    

        // load data -> arg 1 = path to the csv file
        //var data = spark.read.option("header", true).csv(args(0))
        var data = spark.read.option("header", true).csv("E:/upm/BigData/projet_big_data/bigdataproject/src/main/ressources/1987.csv")
        // preprocessing 
        data =data.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
        data.show()
        // model training 
        // model test 
    }
    
    

    def preprocessing(dataToPreProcess: DataFrame) : DataFrame = {

        
        // split train test data ; target ArrDelay 
        //var Array(training, test) = data.randomSplit(Array[Double](0.8, 0.2))

        //training.show()

        // cleaning data 

        // process null val 

        // change categ val ?

        // choose col 

        // choose crossing cols 
        return dataToPreProcess
    }

    def test_model() {
        // print accuracy / tab to choose best parameters ? 

        // compare models 
        
    }
}


