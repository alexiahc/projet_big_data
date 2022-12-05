package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)

        // config de l'app a changer apres 

        val conf = new SparkConf().setAppName("Big Data Project")
        val sc = new SparkContext(conf)

        // exemple 
        val data = sc.textFile("E:/upm/BigData/projet_big_data/bigdataproject/src/main/ressources/1987.csv")
        val numAs = data.filter(line => line.contains("A")).count()
        val numIs = data.filter(line => line.contains("I")).count()
        println(s"Lines with a: ${numAs}, Lines with i: ${numIs}")
        

        // load data -> arg 1 = fichier csv des données 
        // var data = sc.read.option("header", true).csv(args[1])
        //var data = spark.read.option("header", true).csv("/Users/alexi/Documents/GitHub/projet_big_data/2000.csv")
        
        //data = data.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

        // preprocessing 
        
        
        // model training 
        // model test 
    }
    
    

    def preprocessing(){
        // split train test data ; target ArrDelay 
        //var Array(training, test) = data.randomSplit(Array[Double](0.8, 0.2))

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


