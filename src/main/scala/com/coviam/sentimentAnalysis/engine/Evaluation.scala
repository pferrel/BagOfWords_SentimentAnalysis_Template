package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.{AverageMetric, EngineParams, Evaluation, _}


case class Accuracy extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult]{
  override def calculate(q: Query, p: PredictedResult, a: ActualResult): Double = {
    if (p.Sentiment == a.sentiment)
      1.0
    else
      0.0
  }
}


object AccuracyEvaluation extends Evaluation{
    engineMetric = (SA_EngineFactory(), new Accuracy())
}

object EngineParamList extends EngineParamsGenerator{
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParam(appId = 28, evalK = 2))

  engineParamsList = Seq(
    baseEP.copy(algorithmParamsList = Seq(("Naivebayes", AlgorithmParams(1)))),
    baseEP.copy(algorithmParamsList = Seq(("Naivebayes", AlgorithmParams(2))))
  )
}