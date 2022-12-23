
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions.col

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
        
        // exploring data
        x_train.dtypes
        // modifying the columns with object type 

        // !!!!!!!!!! 
        // pb sur les types -> trouver comment changer 
        // de stringType à IntegerType / DoubleType 
        // !!!!!!!!!!

        // pb avec 
        // x_train = x_train.withColumn("DayOfWeek", x_train.select("DayOfWeek").cast(IntegerType))
        // sur cast ?? ne veut pas le détecter 

        val col_obj = List("UniqueCarrier", "TailNum", "Origin", "Dest")

        // ????????????????????
        // persist the database before the for ? 
        // ????????????????????

        for (c<-col_obj) {
            var d = x_train.groupBy(c).count()
            val nb_rows = d.count()

            // for the columns with more then 9 different 
            // possible values, we modify the column to keep 
            // only 10 different values 
            if (nb_rows > 9) {
                d = d.sort(col("count").desc)
                var values_util = d.select(c).map(f=>f.getString(0)).collect.toList
                values_util = values_util.take(9)
                // remplace by "other" the values that aren't 
                // in the list values_util, which are the 9 values 
                // the most frequent in the dataset 
                x_train = x_train.withColumn(c, when(col(c).isin(values_util:_*), col(c)).otherwise("Other"))
                x_test= x_test.withColumn(c, when(col(c).isin(values_util:_*), col(c)).otherwise("Other")) 
            }
        } 
        
        // the cancellation code and the fligh number will not be usefull for the model
        // The column CancellationCode has an information similar to the column DepTime, 
        // because DepTime is null when CancellationCode is not 
        // The FlightNum column gives a number that will not help for the model 
        x_train = x_train.drop("CancellationCode", "FlightNum")

        // If the value in the column DepTime is null, it means that the 
        // fligh never started so the value in the column ArrDelay is also null
        // We keep just the values that are interesting for us for a prediction model, 
        // with not-null values for these two columns, in removing the rows 
        // where ArrayDelay or DepTime is null 
        x_train.filter(!col("DepTime").isNull)
        x_train.filter(!col("ArrDelay").isNull)

        // using the one-hot encoder to create new columns for categorical values 

        // remplacing the null values 

        // removing the target column from the predictors 
        var y_train = x_train.select("ArrDelay")
        var y_test = x_test.select("ArrDelay")
        x_train = x_train.drop("ArrDelay")
        x_test = x_test.drop("ArrDelay")
        x_train = x_train.toDF()
        x_test = x_test.toDF()

        // using algorithms to keep just the necessary columns for the model 

        
    }

    def test_model() {
        // print accuracy / tab to choose best parameters ? 

        // compare models 
        
    }
}


