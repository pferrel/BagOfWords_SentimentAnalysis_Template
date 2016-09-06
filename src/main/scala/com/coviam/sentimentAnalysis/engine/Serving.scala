package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.LServing


class Serving extends LServing[Query, PredictedResult]{
  override def serve(query: Query, predictions: Seq[PredictedResult]): PredictedResult = {
    predictions.head
  }
}

case class PredictedResult(Sentiment: Double, Score : Double) extends Serializable{}
