package upm.bd
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains,col,when,array_union,lit,sum}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.ChiSqSelector


object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)

        // config de l'app a changer apres 

        val spark = SparkSession
            .builder()
            .appName("Big Data Project")
            .enableHiveSupport()
            .getOrCreate()

        // For implicit conversions
        import spark.implicits._

        // exemple 
        //val data = sc.textFile("file:///tmp/book/98.txt")
        // val data = sc.textFile("hdfs:///tmp/book/98.txt")
        // val numAs = data.filter(line => line.contains("a")).count()
        // val numBs = data.filter(line => line.contains("b")).count()
        // println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
        

        // load data -> arg 1 = fichier csv des données 
        // var data = sc.read.option("header", true).csv(args[1])
        var data = spark.read.option("header", true).csv("D:/Cours/2000.csv")
        data = data.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay") 

        // preprocessing 

        //Print the number of missing values for each columns
        data.select(data.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show

        data = data.withColumn("ArrDelay", when(col("ArrDelay")==="NA", null).otherwise(col("ArrDelay"))).withColumn("ArrDelay",col("ArrDelay").cast("Integer"))
        data = data.withColumn("DepDelay", when(col("DepDelay")==="NA", null).otherwise(col("DepDelay"))).withColumn("DepDelay",col("DepDelay").cast("Integer"))

        data.show()
        val col_to_impute = Array("ArrDelay","DepDelay")
        val col_imputed = Array("ArrDelay_imputed","DepDelay_imputed")

        //if we needed to fill missing values we could use the code below but we don't need
        val imputer = new Imputer()
            .setInputCols(col_to_impute)
            .setOutputCols(col_imputed)
            .setStrategy("median")

        data = imputer.fit(data).transform(data)
        data.show()

        //We create the arrays with the name of the categoricals values, we will use them for the one hot encoding.
        val col_obj = Array("UniqueCarrier", "TailNum", "Origin", "Dest")
        val indexed_obj = Array("IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest") //Those features will correspond to the ordinal encoding
        val encoded_obj = Array("NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest") //Those features will correspond to the one hot encoding

        //We extract the list of the values that are less recurent than 1000 values and we give them 'Other' as value
        val inlist = data.groupBy("TailNum").count.orderBy(col("count").desc).where(col("count") < 1000).select("TailNum").map({r => r.getString(0)}).collect.toList

        //We modify all the values that are in inlist and we give them the new value 'Other'
        val df3 = data.withColumn("TailNum", when(data("TailNum").isin(inlist: _*),"Other").otherwise(col("TailNum")))
        df3.show(false)

        df3.groupBy("TailNum").count.orderBy(col("count").desc).show() //To check the number of values in "TailNum"
        
        //We start to do the ordinal encoding because we need it to do the one hot encoding
        val indexer = new StringIndexer()
            .setInputCols(col_obj)
            .setOutputCols(indexed_obj)
            .fit(data)
        val indexed = indexer.transform(data)

        //Then we do the one hot encoding
        val encoder = new OneHotEncoder()
            .setInputCols(indexed_obj)
            .setOutputCols(encoded_obj)

        var data_encoded = encoder.fit(indexed).transform(indexed)
        
        //We drop the temporary column that we used to encode
        data_encoded = data_encoded.drop("UniqueCarrier", "TailNum", "Origin", "Dest","IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest")
        data_encoded.show()

        var Array(x_train, x_test) = data_encoded.randomSplit(Array[Double](0.8, 0.2))
        var y_train = x_train.select("ArrDelay_imputed")
        var y_test = x_test.select("ArrDelay_imputed")
        x_train = x_train.drop("ArrDelay_imputed")
        x_test = x_test.drop("ArrDelay_imputed")
        x_train = x_train.toDF()
        x_test = x_test.toDF()


        val assembler = new VectorAssembler().
            setInputCols(data_encoded.drop("Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "CRSArrTime", "FlightNum", "CRSElapsedTime", "DepDelay_imputed", "Distance", "TaxiOut", "Cancelled", "CancellationCode", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest").columns).
            setOutputCol("features")

        val df4 = assembler.transform(data_encoded)
        df4.show()

        val selector = new ChiSqSelector()
            .setNumTopFeatures(1)
            .setFeaturesCol("features")
            .setLabelCol("ArrDelay_imputed")
            .setOutputCol("selectedFeatures")

        val result = selector.fit(df4).transform(df4)
        result.show()

        // model training 
        // model test 
        spark.stop()
    }
    
    

    def preprocessing(data : DataFrame){
        // split in train / test datasets
        var Array(x_train, x_test) = data.randomSplit(Array[Double](0.8, 0.2))
        
        // exploring data
        x_train.dtypes

        //Print the number of missing values for each columns
        data.select(data.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show

        data = data.withColumn("ArrDelay", when(col("ArrDelay")==="NA", null).otherwise(col("ArrDelay"))).withColumn("ArrDelay",col("ArrDelay").cast("Integer"))
        data = data.withColumn("DepDelay", when(col("DepDelay")==="NA", null).otherwise(col("DepDelay"))).withColumn("DepDelay",col("DepDelay").cast("Integer"))

        // modifying the columns with object type 
        val col_obj = List("UniqueCarrier", "TailNum", "Origin", "Dest")
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
        //We create the arrays with the name of the categoricals values, we will use them for the one hot encoding.
        val col_obj = Array("UniqueCarrier", "TailNum", "Origin", "Dest")
        val indexed_obj = Array("IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest") //Those features will correspond to the ordinal encoding
        val encoded_obj = Array("NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest") //Those features will correspond to the one hot encoding

        //We extract the list of the values that are less recurent than 1000 values and we give them 'Other' as value
        val inlist = data.groupBy("TailNum").count.orderBy(col("count").desc).where(col("count") < 1000).select("TailNum").map({r => r.getString(0)}).collect.toList

        //We modify all the values that are in inlist and we give them the new value 'Other'
        val df3 = data.withColumn("TailNum", when(data("TailNum").isin(inlist: _*),"Other").otherwise(col("TailNum")))

        //We start to do the ordinal encoding because we need it to do the one hot encoding
        val indexer = new StringIndexer()
            .setInputCols(col_obj)
            .setOutputCols(indexed_obj)
            .fit(data)
        val indexed = indexer.transform(data)

        //Then we do the one hot encoding
        val encoder = new OneHotEncoder()
            .setInputCols(indexed_obj)
            .setOutputCols(encoded_obj)

        var data_encoded = encoder.fit(indexed).transform(indexed)
        
        //We drop the temporary column that we used to encode
        data_encoded = data_encoded.drop("UniqueCarrier", "TailNum", "Origin", "Dest","IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest")

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