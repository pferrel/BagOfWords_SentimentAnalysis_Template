package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller._
import org.apache.predictionio.data.storage.{Event, PropertyMap, Storage}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
  * Created by bansarishah on 8/20/16.
  */
class DataSource(val dsp: DataSourceParam) extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, ActualResult]{
  print("data source called -----------")

  def readEvent(sc:SparkContext) : RDD[Sentiment] = {
    val eventDB = Storage.getPEvents()
    val eventRDD: RDD[Event] = eventDB.find(
      appId = dsp.appId,
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
    println("DataSource -- read training called ------")
    new TrainingData(readEvent(sc))
  }

  override def readEval(sc: SparkContext): Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {

    println("read eval called ---")
    val data = readEvent(sc).zipWithIndex()
    var obs = data.take(10)
    for(i <- 0 to obs.size - 1)
      println("obs eval -----" , obs(i))

    (0 until dsp.evalK).map { k =>
      println("cross validation k ---",k)
      val trainigdata = data.filter(_._2 % dsp.evalK != k).map(_._1)
      val testdata = data.filter(_._2 % dsp.evalK == k).map(_._1)
      var trainD = trainigdata.take(3)
    //  (0 until trainD.size).foreach(p => println("training point ---", trainD(p)))
      var testD = testdata.take(3)
    //  (0 until testD.size).foreach(p => println("test point ---", testD(p)))

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
      println("total observation",obs.length)
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
case class DataSourceParam(appId:Int, evalK:Int) extends Params

case class Sentiment(phrase:String, sentiment:Double)

class ActualResult(val sentiment: Double) extends Serializable



// extra code
//    val testAndTrainRDD = data.randomSplit(Array(0.6,0.4))
//    (new TrainingData(testAndTrainRDD.head),
//            new EmptyEvaluationInfo(),
//      testAndTrainRDD.last.map(e => (new Query(e.phrase), new ActualResult(e.sentiment))))

