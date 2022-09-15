package com.liang3z.numeraiexampleinspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

import sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import util._



object NumeraiApp {
  def main(args: Array[String]): Unit =  {
    val tournament_data_path =  "/path/to/your/tournamentdataset"
    val csvfilepath = "/path/to/your/result/csv/filepath"
    val trainingmodelpath = "/path/to/your/model/saving/path"
    val training_data_path = "/path/to/your/trainingdataset/includingfilename"
    val feature_data_path = "/path/to/your/featuresjson/includingfilename"

    val currround = Seq("python",
        "-c",
      "from  numerapi import NumerAPI; napi = NumerAPI(); current_round = napi.get_current_round(tournament=8);print(current_round); "
    ).!!
    val tournament_data = tournament_data_path+"/tournament_data_"+currround.stripTrailing()+".parquet"

    val spark = SparkSession.builder().appName("Numerai Example").config("spark.default.parallelism","128").config("spark.sql.shuffle.partitions", "128").getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val realcsvfilepath = new Path(csvfilepath)
    if (fs.exists(realcsvfilepath)) fs.delete(realcsvfilepath, true)
    val modelpath = new Path(trainingmodelpath)

    val colname = "feature_sets.medium"
    val featuredata = spark.read.json(feature_data_path).select(colname)
    val feature_cols = featuredata.collect().toList.apply(0).toString().stripPrefix("[ArraySeq(").stripSuffix(")]").split(", ").toList
    val all_data_cols =  "id" :: feature_cols ::: List(ERA_COL, DATA_TYPE_COL, TARGET_COL)
    val trainingdata_df = spark.read.parquet(training_data_path).select(all_data_cols.map(col): _*)

    val riskestfeatures =  spark_get_biggest_features(trainingdata_df, 20)

    val vecassem = new VectorAssembler().setInputCols(feature_cols.toArray).setOutputCol("featuresvec")
    val model1 = { if (fs.exists(modelpath)) {GBTRegressionModel.load(modelpath.toString)}
                     else {
    val gbt =  new GBTRegressor()
      .setStepSize(0.001)
      .setFeaturesCol("featuresvec")
      .setLabelCol(TARGET_COL)
      .setMaxDepth(2)
      .setMaxIter(10)
      .setFeatureSubsetStrategy("1.0")
      val traincol = "id"::"featuresvec"::TARGET_COL::Nil
    val transformed_df = vecassem.transform(trainingdata_df).select(traincol.map(col):_*)
    val model1 = gbt.fit(transformed_df)
     model1.save(trainingmodelpath)
    model1
      }
    }

    val all_cols_tournament = ("id"::ERA_COL::feature_cols)
    val tournament_data_df =spark.read.parquet(tournament_data).select(all_cols_tournament.map(col):_*)

    val tournament_data_nona_df =  tournament_data_df.na.fill(0.5, feature_cols)
    val all_cols_withpredict = ("id"::ERA_COL::"prediction"::riskestfeatures)
    val tournamentdata_predict = model1.transform(vecassem.transform(tournament_data_nona_df)).select(all_cols_withpredict.map(col):_*)

    val predict_neutralized_date =  spark_neutralize(tournamentdata_predict, "prediction", 1.0, true, ERA_COL)
    predict_neutralized_date.coalesce(1).write.option("header", true).csv(csvfilepath)

    fs.close()
    spark.stop()
  }
}