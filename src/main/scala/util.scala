package com.liang3z.numeraiexampleinspark

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.{abs, array, col, corr, count, explode, lit, map, map_concat, mean, row_number, udaf, udf, when}
import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.Window

object util {
  val ERA_COL = "era"
  val TARGET_COL = "target_nomi_20"
  val DATA_TYPE_COL = "data_type"

  def spark_get_biggest_features(trainingdata_df: DataFrame, n: Int): List[String] = {

    val funclists = trainingdata_df.columns.filter(name => name startsWith ("feature")).toVector.map(x => corr(x, TARGET_COL))
    val corrdf = trainingdata_df.groupBy(col(ERA_COL)).agg(funclists.head, funclists.tail: _*)
    val thelength = corrdf.count() / 2
    val corrdf1 = corrdf.sort(col(ERA_COL)).withColumn("litid", lit(1)).withColumn("sid", row_number().over(Window.partitionBy(col("litid")).orderBy("litid")))
    val corrdf2 = corrdf1.withColumn("era_partition", when(col("sid") <= thelength, 0).otherwise(1)).drop(col("litid"))
    val corrcols = corrdf2.columns.filter(name => name startsWith ("corr")).toVector.map(x => mean(x))
    val meandf1 = corrdf2.groupBy("era_partition").agg(corrcols.head, corrcols.tail:_*)
    val new_col = meandf1.columns.toVector.map(f => col(f).as(s"${f}_2"))
    val meandf2 = meandf1.join(meandf1.select(new_col: _*), col("era_partition") === (col("era_partition_2") - 1), "inner").drop("era_partition_2", "era_partition")
    val abscol = meandf2.columns.toVector.filter(pred => pred.endsWith("_2")).zip(meandf2.columns.toVector.filter(pred => !pred.endsWith("_2"))).map(f => abs(col(f._1) - col(f._2)))
    val absdf = meandf2.select(abscol: _*)
    val feacoll = absdf.columns.toVector.zip(absdf.columns.toVector.map(f => StringUtils.substringBetween(f, "abs((avg(corr(", ","))).map(f => col(f._1).as(f._2))
    val feaabsdf = absdf.select(feacoll: _*)
    val feanamecol = feaabsdf.columns.toVector.map(f => col(f)).concat(feaabsdf.columns.toVector.map(f => lit(f).as(f + "_name")))
    val feanamedf = feaabsdf.select(feanamecol: _*)
    val mapcol = feanamedf.columns.toVector.filter(pred => pred.endsWith("_name")).zip(feanamedf.columns.toVector.filterNot(pred => pred.endsWith("_name"))).map(f => map(col(f._1), col(f._2)))
    val mapfeadf = feanamedf.select(mapcol: _*)
    val allmapdf = mapfeadf.select(map_concat(mapfeadf.columns.toVector.map(col(_)): _*))
    val allnamedf = allmapdf.withColumnRenamed(allmapdf.columns.toList.apply(0), "allmap").withColumn("id", lit(1))
    val finaldf = allnamedf.select(col("id"), explode(col("allmap"))).sort(col("value").desc).limit(n).toDF()
    finaldf.select(col("key")).collect().toList.map(f => f.toString().stripPrefix("[").stripSuffix("]"))
  }

  val normalizetheval = (rank: Int, cnt: Long) => {
    val normdis = new NormalDistribution()
    val norvaal = (rank.toDouble - 0.5) / cnt.toDouble
    normdis.inverseCumulativeProbability(norvaal)
  }

  def spark_neutralize(df: DataFrame, columns: String, proportion: Double, normalize: Boolean, era_col: String): DataFrame = {
    val featurecol = df.columns.toList.filter(f => f.startsWith("feature")).map(col(_))
    val fearrdf = df.withColumn("featurearray", array(featurecol: _*)).withColumn("litcol", lit(1)).withColumn("sn", row_number().over(Window.partitionBy(col("litcol")).orderBy(col("litcol"))))
    val rankdf = fearrdf.withColumn("row_number", row_number().over(Window.partitionBy(era_col).orderBy(columns)))
    val cntdf = rankdf.groupBy(col(era_col)).count().withColumnRenamed(era_col, "era_2")
    val rankcntdf = rankdf.join(cntdf, col(era_col) === col("era_2"), "inner").drop("era_2").withColumnRenamed("count", "count(eachera)").sort(col("sn"))
    val normudf = udf(normalizetheval)
    val normalizeddf = normalize match {
      case true => rankcntdf.withColumn("normalizedval", normudf(col("row_number"), col("count(eachera)")))
      case false => rankcntdf.withColumn("normalizedval", col(columns))
    }
    val propnormdf = normalizeddf.withColumn("proportion", lit(proportion)).select(col("id"), col("era"), col("featurearray"), col("sn"), col("normalizedval"), col("proportion"))
    val erapropdf = propnormdf.groupBy(col(era_col)).min("proportion").withColumnRenamed("min(proportion)", "proportion").withColumnRenamed("era", "era_3")
    val group1df = propnormdf.withColumn("snfea", map(col("sn"), col("featurearray"))).drop(col("featurearray"))
    val fedudaf = udaf(generafeadata)
    val featuregrdf = group1df.groupBy(col(era_col)).agg(fedudaf(col("snfea"))).withColumnRenamed("generafeadata$(snfea)", "featuremap")
    val group2df = propnormdf.withColumn("snnorm", map(col("sn"), col("normalizedval"))).drop(col("featurearray"))
    val normudaf = udaf(generanormdata)
    val normgrdf = group2df.groupBy(col(era_col)).agg(normudaf(col("snnorm"))).withColumnRenamed("generanormdata$(snnorm)", "normap").withColumnRenamed("era", "era_2")
    val feanormdf = featuregrdf.join(normgrdf, col("era") === col("era_2"), "inner").drop("era").join(erapropdf, col("era_2") === col("era_3"), "inner")
    val maxres = udf(Neutralization.neutralization)
    val neutradf = feanormdf.sort(col("era_2")).withColumn("neutrares", maxres(col("featuremap"), col("normap"), col("proportion"))).select(col("era_2"), col("neutrares"))
    val windowSpecs = Window.partitionBy(col("litid")).orderBy(col("litid"))
    val neutrasn1df = neutradf.select(col("era_2"), explode(col("neutrares"))).withColumnRenamed("col", "neutralization").withColumn("litid", lit(1)).withColumn("sn2", row_number().over(windowSpecs))
    val neutrasnperdf = neutrasn1df.withColumn("percentsn", row_number().over(Window.partitionBy(col("litid")).orderBy(col("neutralization"))))
    val wcntdf = neutrasnperdf.groupBy(col("litid")).count().withColumnRenamed("litid", "litid_2")
    val neutrasndf = neutrasnperdf.join(wcntdf, col("litid") === col("litid_2"), "inner").withColumn("percentdata", col("percentsn").cast(DoubleType) / col("count").cast(DoubleType)).sort(col("sn2")).select(col("sn2"), col("percentdata"))
    val idsndf = df.select(col("id")).withColumn("litid", lit(1)).withColumn("sn", row_number().over(windowSpecs))
    val finaldf = idsndf.join(neutrasndf, col("sn") === col("sn2"), "inner").sort(col("sn")).select(col("id"), col("percentdata")).withColumnRenamed("percentdata", "prediction")
    finaldf
  }
}