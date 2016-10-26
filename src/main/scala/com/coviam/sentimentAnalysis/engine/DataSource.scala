package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller._
import org.apache.predictionio.data.storage.{Event, Storage}
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD



class DataSource(val dsp: DataSourceParam) extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult]{

  def readEvent(sc:SparkContext) : RDD[Sentiment] = {
    val eventRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("phrase"),
      eventNames = Some(List("train"))
      )(sc)
    val sentimentRDD : RDD[Sentiment] = eventRDD.map{
      event  =>
        val phrase = event.properties.get[String]("phrase")
        val sentiment = event.properties.get[Double]("sentiment")
        Sentiment(phrase,sentiment)
    }
    sentimentRDD
  }
   override def readTraining(sc: SparkContext): TrainingData = {
    new TrainingData(readEvent(sc))
  }

  override def readEval(sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {

    val data = readEvent(sc).zipWithIndex()
    (0 until dsp.evalK).map { k =>
      val trainigdata = data.filter(_._2 % dsp.evalK != k).map(_._1)
      val testdata = data.filter(_._2 % dsp.evalK == k).map(_._1)
     (new TrainingData(trainigdata),
        new EmptyEvaluationInfo(),
        testdata.map(e => (new Query(e.phrase), new ActualResult(e.sentiment))))
    }
  }
}

class TrainingData(val phraseAndSentiment:RDD[Sentiment]) extends Serializable with SanityCheck{

  def sanityCheck(): Unit = {
    try {
      val obs = phraseAndSentiment.takeSample(false, 5)
      println("sample observation",obs.length)
      (0 until 5).foreach(
        k => println("Observation " + (k + 1) + " label: " + obs(k))
      )
      println()
    } catch {
      case (e: ArrayIndexOutOfBoundsException) => {
        println()
        println("Data set is empty, make sure event fields match imported data.")
        println()
      }
    }
  }

}
case class Query(phrase:String) extends Serializable
case class DataSourceParam(appName:String, evalK:Int) extends Params

case class Sentiment(phrase:String, sentiment:Double)

class ActualResult(val sentiment: Double) extends Serializable

