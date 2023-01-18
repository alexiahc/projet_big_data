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
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

// NOUVEAUX IMPORT À AJOUTER 
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressor

object MyApp {
    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.WARN)

        // Application configuration 

        val spark = SparkSession
            .builder()
            .appName("Big Data Project")
            .enableHiveSupport()
            .getOrCreate()

        // For implicit conversions
        import spark.implicits._ 

        // We load the dataset from the csv file entered in arguments 
        var data = spark.read.option("header", true).csv(args(0))
        // var data = spark.read.option("header", true).csv("/Users/alexi/Documents/GitHub/projet_big_data/2000.csv")

        //We remove the features that are forbidden
        data = data.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay") 

        // **************************************************************************
        // **************************************************************************
        // preprocessing on the dataset 
        // **************************************************************************
        // **************************************************************************
        
        // split in train / test datasets
        var Array(train, test) = data.randomSplit(Array[Double](0.8, 0.2))

        // **************************************************************************
        // DROP ROWS AND COLUMNS NOT USEFUL 

        // If the value in the column DepTime is null, it means that the 
        // fligh never started so the value in the column ArrDelay is also null
        // We keep just the values that are interesting for us for a prediction model, 
        // with not-null values for these two columns, in removing the rows 
        // where ArrayDelay or DepTime is null 
        train = train.filter(!col("DepTime").isNull)
        train = train.filter(!col("ArrDelay").isNull)
        train = train.filter(!(col("DepTime")==="NA" || col("DepTime")==="null"))
        train = train.filter(!(col("ArrDelay")==="NA" || col("ArrDelay")==="null"))

        test = test.filter(!col("DepTime").isNull)
        test = test.filter(!col("ArrDelay").isNull)
        test = test.filter(!(col("DepTime")==="NA" || col("DepTime")==="null"))
        test = test.filter(!(col("ArrDelay")==="NA" || col("ArrDelay")==="null"))

        // the cancellation and the fligh number will not be usefull for the model
        // The column CancellationCode has an information similar to the column DepTime, 
        // because DepTime is null when CancellationCode is not 
        // Cancelled is true only when the departure time is null so it will not 
        // be useful 
        // The FlightNum column gives a number that will not help for the model 
        train = train.drop("Cancelled", "CancellationCode", "FlightNum")
        test = test.drop("Cancelled", "CancellationCode", "FlightNum")

        // We also can delete the year as it will not vary if we analyze only one dataset (we could 
        // concatenate all the dataset and make one big analysis on all the files
        // however it will use too much computational power and we do not have a real spark cluster)
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// EN VRAI A GARDER PCQ SI LE PROF CONCATENE PLUSIEURS ANNEES CA PEUT ETRE UTILE 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //train = train.drop("Year")
        // test = test.drop("Year")

        // **************************************************************************
        // IMPUTER FOR THE NUMERICAL COLUMNS 

        // We are filling the missing values, we create an array with all the numerical values that we want to fill
        val col_to_impute = Array("Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime",
        "CRSElapsedTime","Distance", "TaxiOut")
        val col_imputed = Array("Month_imputed","DayofMonth_imputed","DayOfWeek_imputed","CRSDepTime_imputed",
        "CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", "TaxiOut_imputed")
        
        //We make sure that scala read them as Integer
        for (c<-col_to_impute){
            var d = train.withColumn("isNotNull", col(c).isNotNull)
            train = train.withColumn(c,col(c).cast("Integer"))
            test = test.withColumn(c,col(c).cast("Integer"))
        }

        //To fill the missing values we are using the median of each columns 
        val imputer = new Imputer()
            .setInputCols(col_to_impute)
            .setOutputCols(col_imputed)
            .setStrategy("median")

        //We then fill the values for both dataset with the median of the training set
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PASSE DANS UN PIPELINE DU TOUT 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //var imputer_fit = imputer.fit(train)
        //train = imputer_fit.transform(train)
        //test = imputer_fit.transform(test)

        // **************************************************************************
        // ONE HOT ENCODING FOR THE CATEGORICAL COLUMNS 

        // Modifying the columns with object type 
        val col_obj = Array("UniqueCarrier", "TailNum", "Origin", "Dest")

        // Those features will correspond to the ordinal encoding used in the one hot encoder 
        val indexed_obj = Array("IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest") 
        // Those features will correspond to the one hot encoding
        val encoded_obj = Array("NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest") 
        
        // For the columns with more then 9 different possible values, we modify the column 
        // to keep only 10 different values maximum 
        for (c<-col_obj) {
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// FAIRE UN CACHE DU D  POUR EVITER DE RECALCULER LE DATA SET 
// peut etre chercher un autre moyen de le faire pour eviter des 
// calculs en plus ? 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            var d = train.groupBy(c).count().persist()
            val nb_rows = d.count()

            if (nb_rows > 9) {
                d = d.sort(col("count").desc)
                var values_util = d.select(c).map(f=>f.getString(0)).collect.toList
                values_util = values_util.take(9)
                // We remplace by "other" the values that aren't 
                // in the list values_util, which are the 9 values 
                // the most frequent in the dataset 
                train = train.withColumn(c, when(col(c).isin(values_util:_*), col(c)).otherwise("Other"))
                test= test.withColumn(c, when(col(c).isin(values_util:_*), col(c)).otherwise("Other")) 
            }
            d = d.unpersist()
        }


        //One hot encoding, to fit the one hot encoding model we are using the training set
        //We start to do the ordinal encoding because we need it to do the one hot encoding
        val indexer = new StringIndexer()
            .setInputCols(col_obj)
            .setOutputCols(indexed_obj)

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PASSE DANS UN PIPELINE DU TOUT 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //    .fit(train)
        //val indexed_train = indexer.transform(train)
        //val indexed_test = indexer.transform(test)

        //Then we do the one hot encoding
        val encoder = new OneHotEncoder()
            .setInputCols(indexed_obj)
            .setOutputCols(encoded_obj)

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PASSE DANS UN PIPELINE DU TOUT 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // var encoder_fit = encoder.fit(indexed_train)
        // train = encoder_fit.transform(indexed_train)
        // test = encoder_fit.transform(indexed_test)

        // **************************************************************************
        // CLEANING THE DATASET 

        // We only keep the new preprocess features and get rides of the old ones
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// AVEC UN PIPELINE COMPLET ON LAISSE TOUTES LES COLONNES MEME CELLES PAS 
// UTILISEES DANS LES MODELS SINON LES CALCULS VONT PLANTER 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // train = train.drop("UniqueCarrier", "TailNum", "Origin", "Dest", "IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest",
        //                "Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","Distance", "TaxiOut")
        // test = test.drop("UniqueCarrier", "TailNum", "Origin", "Dest", "IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest",
        //                "Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","Distance", "TaxiOut")

        // Column that have not been casted as integers before 
        train = train.withColumn("DepTime",col("DepTime").cast("Integer"))
        test = test.withColumn("DepTime",col("DepTime").cast("Integer"))
        train = train.withColumn("DepDelay",col("DepDelay").cast("Integer"))
        test = test.withColumn("DepDelay",col("DepDelay").cast("Integer"))

        // To be able to use the dataset in a Machine Learning model we need to create a column name 
        // "features" who is a list of all the usefull features
        val assembler = new VectorAssembler()
            .setInputCols(Array("DepTime","DepDelay","Month_imputed","DayofMonth_imputed","DayOfWeek_imputed",
            "CRSDepTime_imputed","CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", 
            "TaxiOut_imputed", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest"))
            .setOutputCol("features")

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PASSE DANS UN PIPELINE DU TOUT 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // train = assembler.transform(train)
        // test = assembler.transform(test)

        // We can now get rid of the columns copied in features
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// AVEC UN PIPELINE COMPLET ON LAISSE TOUTES LES COLONNES MEME CELLES PAS 
// UTILISEES DANS LES MODELS SINON LES CALCULS VONT PLANTER 
// rajouté après le pipeline pour la mémoire 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // train = train.drop("DepTime","DepDelay","Month_imputed","DayofMonth_imputed","DayOfWeek_imputed",
        //     "CRSDepTime_imputed","CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", 
        //     "TaxiOut_imputed", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest")
        // test = test.drop("DepTime","DepDelay","Month_imputed","DayofMonth_imputed","DayOfWeek_imputed",
        //     "CRSDepTime_imputed","CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", 
        //     "TaxiOut_imputed", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest")

        // We make sure that the label features is read by scala as a double 
        train = train.withColumn("ArrDelay",col("ArrDelay").cast("double"))
        test = test.withColumn("ArrDelay",col("ArrDelay").cast("double"))

        // **************************************************************************
        // **************************************************************************
        // Training and testing regression models to predict the ArrDelay 
        // **************************************************************************
        // **************************************************************************

        // We use the chisquare test to chose the 5 best features
        val selector = new ChiSqSelector()
            .setNumTopFeatures(5)
            .setFeaturesCol("features")
            .setLabelCol("ArrDelay")
            .setOutputCol("featuresSelected")

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PASSE DANS UN PIPELINE DU TOUT 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // val selectorModel = selector.fit(train)
        // train = selectorModel.transform(train)
        // test = selectorModel.transform(test)

        // create pipeline for preprocessing and selecting features 
        val pipeline = new Pipeline()
            .setStages(Array(imputer, indexer, encoder, assembler, selector))

        var pipeline_fit = pipeline.fit(train)
        train = pipeline_fit.transform(train)
        test = pipeline_fit.transform(test)

        // We replace the old features by the selected ones
        train = train.drop("features")
        test = test.drop("features")
        train = train.withColumnRenamed("featuresSelected","features")
        test = test.withColumnRenamed("featuresSelected","features")

        // drop all unused columns 
        train = train.drop("DepTime","DepDelay","Month_imputed","DayofMonth_imputed","DayOfWeek_imputed",
            "CRSDepTime_imputed","CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", 
            "TaxiOut_imputed", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest")
        test = test.drop("DepTime","DepDelay","Month_imputed","DayofMonth_imputed","DayOfWeek_imputed",
            "CRSDepTime_imputed","CRSArrTime_imputed","CRSElapsedTime_imputed","Distance_imputed", 
            "TaxiOut_imputed", "NumUniqueCarrier", "NumTailNum", "NumOrigin", "NumDest")
        train = train.drop("UniqueCarrier", "TailNum", "Origin", "Dest", "IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest",
                       "Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","Distance", "TaxiOut")
        test = test.drop("UniqueCarrier", "TailNum", "Origin", "Dest", "IndexUniqueCarrier", "IndexTailNum", "IndexOrigin", "IndexDest",
                       "Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","CRSElapsedTime","Distance", "TaxiOut")


        // **************************************************************************
        // LINEAR REGRESSION MODEL 

        // creating the model 
        val lr = new LinearRegression()
            .setLabelCol("ArrDelay")
            .setFeaturesCol("features")
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
        val lrModel = lr.fit(train)

        // Print the coefficients and intercept for linear regression 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// EN VRAI PAS LA PEINE JUSTE LES RESULTATS FINAUX C OK , QUE L'ESSENTIEL 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        // Summarize the model over the training set and print out some metrics
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// PARTIE A GARDER OU NON ? BESOIN QUE DES TESTS DE PRÉDICTIONS ? 
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // val trainingSummary = lrModel.summary
        // println(s"numIterations: ${trainingSummary.totalIterations}")
        // println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        // //trainingSummary.residuals.show()
        // println(s"R Squared (R2) on train data: ${trainingSummary.r2}")
        // println(s"Root Mean Squared Error (RMSE) on test data: ${trainingSummary.rootMeanSquaredError}")

        // prediction of the model on the test dataset 
        val prediction = lrModel.transform(test)
        //prediction.select("prediction","ArrDelay","features").show(20)

        // evaluate the prediction on the test dataset with R2 metric 
        val regEvalR2 = new RegressionEvaluator()
            .setMetricName("r2")
            .setLabelCol("ArrDelay")
            .setPredictionCol("prediction")

        val r2_test = regEvalR2.evaluate(prediction)
    
        // evaluate the prediction on the test dataset with RMSE metric 
        val regEvalRMSE = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("ArrDelay")
            .setPredictionCol("prediction")

        val rmse_test = regEvalRMSE.evaluate(prediction)

        println(s"R Squared (R2) on test data: ${r2_test}")
        println(s"Root Mean Squared Error (RMSE) on test data: ${rmse_test}")

        // **************************************************************************
        // RANDOM FOREST REGRESSION MODEL 

        // creating the model 
        val rf = new RandomForestRegressor()
            .setLabelCol("ArrDelay")
            .setFeaturesCol("features")
            .setSeed(42)
        val rfModel = rf.fit(train)

        // prediction of the model on the test dataset 
        val prediction = rfModel.transform(test)
        //prediction.select("prediction","ArrDelay","features").show(20)
        
        val r2_test = regEvalR2.evaluate(prediction)
        println(s"R Squared (R2) on test data: ${r2_test}")

        val rmse_test = regEvalRMSE.evaluate(prediction)
        println(s"Root Mean Squared Error (RMSE) on test data: ${rmse_test}")

        // **************************************************************************
        // GBT REGRESSOR MODEL 

        // creating the model 
        val gbt = new GBTRegressor()
            .setLabelCol("ArrDelay")
            .setFeaturesCol("features")
            .setMaxIter(5)
            .setMinWeightFractionPerNode(0.049)
        val gbtModel = gbt.fit(train)

        // prediction of the model on the test dataset 
        val prediction = gbtModel.transform(test)
        //prediction.select("prediction","ArrDelay","features").show(20)
        
        val r2_test = regEvalR2.evaluate(prediction)
        println(s"R Squared (R2) on test data: ${r2_test}")

        val rmse_test = regEvalRMSE.evaluate(prediction)
        println(s"Root Mean Squared Error (RMSE) on test data: ${rmse_test}")

        // **************************************************************************
        // DECISIONS TREE REGRESSOR 

         // creating the model 
        val dt = new DecisionTreeRegressor()
            .setLabelCol("ArrDelay")
            .setFeaturesCol("features")
        val dtModel = dt.fit(train)

        // prediction of the model on the test dataset 
        val prediction = dtModel.transform(test)
        //prediction.select("prediction","ArrDelay","features").show(20)
        
        val r2_test = regEvalR2.evaluate(prediction)
        println(s"R Squared (R2) on test data: ${r2_test}")

        val rmse_test = regEvalRMSE.evaluate(prediction)
        println(s"Root Mean Squared Error (RMSE) on test data: ${rmse_test}")
        
        spark.stop()
    }
}