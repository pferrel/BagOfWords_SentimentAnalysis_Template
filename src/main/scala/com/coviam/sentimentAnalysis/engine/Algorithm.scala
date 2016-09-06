package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.{P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}



class Algorithm(val ap:AlgorithmParams) extends P2LAlgorithm[PreparedData, NaiveBayesModel, Query, PredictedResult]{

  override def train(sc: SparkContext, pd: PreparedData): NaiveBayesModel = {
    NaiveBayes.train(pd.labeledpoints,lambda = ap.lambda)
  }


  override def predict(model: NaiveBayesModel, query: Query): PredictedResult = {

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


