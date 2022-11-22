
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)

        // config de l'app a changer apres 

        val conf = new SparkConf().setAppName("My first Spark application")
        val sc = new SparkContext(conf)

        // exemple 
        //val data = sc.textFile("file:///tmp/book/98.txt")
        // val data = sc.textFile("hdfs:///tmp/book/98.txt")
        // val numAs = data.filter(line => line.contains("a")).count()
        // val numBs = data.filter(line => line.contains("b")).count()
        // println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
        

        // load data -> arg 1 = fichier csv des données 
        // var data = sc.read.option("header", true).csv(args[1])
        var data = spark.read.option("header", true).csv("/Users/alexi/Documents/GitHub/projet_big_data/2000.csv")
        
        data = data.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

        // preprocessing 
        
        
        // model training 
        // model test 
    }
    
    

    def preprocessing(data :){
        // split train test data ; target ArrDelay 
        var Array(training, test) = data.randomSplit(Array[Double](0.8, 0.2))

        // cleaning data 

        // process null val 

        // change categ val ?

        // choose col 

        // choose crossing cols 

    }

    def test_model() {
        // print accuracy / tab to choose best parameters ? 

        // compare models 
        
    }
}


