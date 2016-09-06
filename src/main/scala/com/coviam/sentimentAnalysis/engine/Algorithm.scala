package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

/**
  * Created by bansarishah on 8/20/16.
  */
class Algorithm(val ap:AlgorithmParams) extends P2LAlgorithm[PreparedData, NaiveBayesModel, Query, PredictedResult]{

  override def train(sc: SparkContext, pd: PreparedData): NaiveBayesModel = {
   println("trainig classiifer ------")
    NaiveBayes.train(pd.labeledpoints,lambda = ap.lambda)
  }


  override def predict(model: NaiveBayesModel, query: Query): PredictedResult = {

    println("prediction called ---")
    val sc_new = SparkContext.getOrCreate()

    val sqlContext = SQLContext.getOrCreate(sc_new)
    val phraseDataframe = sqlContext.createDataFrame(Seq(query)).toDF("phrase")
    phraseDataframe.show()

    val dpObj = new DataPreparator
    val tf = dpObj.processPhrase(phraseDataframe)
    val labeledpoints = tf.map(row => row.getAs[Vector]("rowFeatures"))

    val predictedResult = model.predict(labeledpoints)
    val result = predictedResult.first()

    val prob = model.predictProbabilities(labeledpoints)
    val score = prob.first().toArray

    var weight:Double = 0.0
    if(result == 1.0)
      {weight = score.last}
    else
      {weight = score.head}

    PredictedResult(result,weight)

  }

}

case class AlgorithmParams(val lambda:Double) extends Params


//    extra code ---
//    val tokenizer = new Tokenizer().setInputCol("phrase").setOutputCol("words")
//    val tokenizedDF = tokenizer.transform(phraseDataframe)
//    tokenizedDF.show()
//
//    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
//    val stopRemoveDF = remover.transform(tokenizedDF)
//    stopRemoveDF.show()
//
//    val ngram = new NGram().setN(1).setInputCol("filtered").setOutputCol("ngrams")
//    val ngramDataFrame = ngram.transform(stopRemoveDF)
//    ngramDataFrame.show()
//
//    var htf = new HashingTF().setNumFeatures(10000).setInputCol("ngrams").setOutputCol("rowFeatures")
//    val tf = htf.transform(ngramDataFrame)
//    tf.show()
//
//    println("before labelling points in prediction")

//    predictedResult.persist()
//    val BDCresult = sc_new.broadcast(predictedResult.collect())
//    val result = predictedResult
//    val result = predictedResult.first()