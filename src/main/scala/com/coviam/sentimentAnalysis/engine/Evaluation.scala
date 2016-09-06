package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller
import org.apache.predictionio.controller.{AverageMetric, EngineParams, Evaluation, _}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


case class Accuracy extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult]{
  println("accuracy called ------")
  override def calculate(q: Query, p: PredictedResult, a: ActualResult): Double = {
    println("calculate accuracy --",p.Sentiment,a.sentiment)
    if (p.Sentiment == a.sentiment)
      1.0
    else
      0.0
  }
}



//class AccuracyMetric extends Metric[EmptyEvaluationInfo, Query, PredictedResult, Sentiment, Double] {
//
//  override def calculate( sc: SparkContext,
//                          data: Seq[(EmptyEvaluationInfo, RDD[(Query, PredictedResult, Sentiment)])]): Double = {
//
//    val accurate = sc.accumulator(0)
//    val inaccurate = sc.accumulator(0)
//
//    data.foreach { set =>
//      set._2.foreach { one =>
//        val predicted = one._2
//        val actual = one._3.sentiment
//
//        if (predicted == actual) {
//          accurate += 1
//        } else {
//          inaccurate += 1
//        }
//      }
//    }
//
//    accurate.value.toDouble / (accurate.value.toDouble + inaccurate.value.toDouble)
//  }
//}


object AccuracyEvaluation extends Evaluation{
    println("accuracy evaluation called ----")
    engineMetric = (SA_EngineFactory(), new Accuracy())
}

object EngineParamList extends EngineParamsGenerator{
  println("engine paramlist called ------")
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParam(appId = 28, evalK = 2))

  engineParamsList = Seq(
    baseEP.copy(algorithmParamsList = Seq(("Naivebayes", AlgorithmParams(1)))),
    baseEP.copy(algorithmParamsList = Seq(("Naivebayes", AlgorithmParams(2))))
  )
}