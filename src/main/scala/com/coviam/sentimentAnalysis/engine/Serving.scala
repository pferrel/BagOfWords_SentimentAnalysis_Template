package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.LServing

/**
  * Created by bansarishah on 8/20/16.
  */
class Serving extends LServing[Query, PredictedResult]{
  override def serve(query: Query, predictions: Seq[PredictedResult]): PredictedResult = {
   // println("predicted result -- serving layer --", predictions.head)
    predictions.head
  }
}

case class PredictedResult(Sentiment: Double, Score : Double) extends Serializable{}
