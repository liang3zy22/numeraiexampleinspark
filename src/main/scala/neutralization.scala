package com.liang3z.numeraiexampleinspark


import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import breeze.linalg.{DenseMatrix => Brdm, pinv => BrPinv}
import breeze.stats.stddev
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class featuredata(features: Map[Int, Array[Double]])
case class allfeatturedata(var featuredatas: Map[Int,Array[Double]])
case class normdata(data: Map[Int, Double])
case class allnormdata(var datas: Map[Int, Double])

object generanormdata extends Aggregator[normdata, allnormdata, Map[Int,Double]] {
  override def zero: allnormdata = allnormdata(Map[Int, Double]())

  override def reduce(b: allnormdata, a: normdata): allnormdata = {
    b.datas = b.datas.concat(a.data)
    b
  }

  override def merge(b1: allnormdata, b2: allnormdata): allnormdata = {
    b1.datas = b1.datas.concat(b2.datas)
    b1
  }

  override def finish(reduction: allnormdata): Map[Int, Double] = reduction.datas

  override def bufferEncoder: Encoder[allnormdata] = Encoders.product

  override def outputEncoder: Encoder[Map[Int, Double]] = ExpressionEncoder[Map[Int, Double]]()
}


object generafeadata extends Aggregator[featuredata, allfeatturedata, Map[Int,Array[Double]]] {
  override def zero: allfeatturedata = allfeatturedata(Map[Int, Array[Double]]())

  override def reduce(b: allfeatturedata, a: featuredata): allfeatturedata = {
    b.featuredatas = b.featuredatas.concat(a.features)
    b
  }

  override def merge(b1: allfeatturedata, b2: allfeatturedata): allfeatturedata = {
    b1.featuredatas = b1.featuredatas.concat(b2.featuredatas)
    b1
  }

  override def finish(reduction: allfeatturedata): Map[Int,Array[Double]] = reduction.featuredatas

  override def bufferEncoder: Encoder[allfeatturedata] = Encoders.product

  override def outputEncoder: Encoder[Map[Int,Array[Double]]] = ExpressionEncoder[Map[Int, Array[Double]]]()
}


object Neutralization  {
 val neutralization = (featuremap: Map[Int, Array[Double]], normmap: Map[Int, Double], proportion: Double) => {
    val feaarray = featuremap.toArray.sortBy(x => x._1).map(y => y._2)
    val colcnt = feaarray.apply(0).length
    val featureax = new Brdm[Double](feaarray.length, colcnt, feaarray.flatten, 0, colcnt, true)

   val normarray = normmap.toArray.sortBy(x => x._1).map(y => y._2)
    val normedmax = new Brdm[Double](normarray.length, 1, normarray)

   val tmpmax =  featureax *(BrPinv(featureax) * normedmax)
   val tmp2max = normedmax - (tmpmax *:* proportion)
   val finalmax = tmp2max /stddev((tmp2max))
    finalmax.toArray
  }
}
