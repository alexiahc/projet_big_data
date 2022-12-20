
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

        var Array(x_train, x_test) = data.randomSplit(Array[Double](0.8, 0.2))
        var y_train = x_train.select("ArrDelay")
        var y_test = x_test.select("ArrDelay")
        x_train = x_train.drop("ArrDelay")
        x_test = x_test.drop("ArrDelay")
        x_train = x_train.toDF()
        x_test = x_test.toDF()

        // exploring data
        x_train.dtypes
        // modifying the columns with object type 

        // !!!!!!!!!! 
        // pb sur les types -> trouver comment changer 
        // de stringType à IntegerType / DoubleType 
        // !!!!!!!!!!



        val col_obj = List("UniqueCarrier", "TailNum", "Origin", "Dest")

        // ????????????????????
        // persist the database before the for ? 
        // ????????????????????

        for (c<-col_obj) {
            var d = x_train.groupBy(c).count()
            val nb_rows = d.count()
            if (nb_rows > 10) {
                d = d.sort(col("count").desc)
                val cols = d.select(c).take(5)
                // remplace by "other" the values that aren't 
                // in the list cols 
                // map on the column 
                


            }

        } 
        // for the columns with more then 10 different 
        // possible values, we modify the column to keep 
        // only 10 different values 

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


