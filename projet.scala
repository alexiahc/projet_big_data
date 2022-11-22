
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
        val data = sc.read.csv(args[1])
        // preprocessing 
        // model training 
        // model test 
    }
    
    def delete_cols(data : DataFrame) {
        cols = [ArrTime, ActualElapsedTime, AirTime, TaxiIn, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay]
        for (col in cols){
            data supp col 
        }
    }

    def preprocessing(data :){
        // split train test data 

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


