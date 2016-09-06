package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{Tokenizer, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.callUDF

/**
  * Created by bansarishah on 8/20/16.
  */
class DataPreparator() extends PPreparator[TrainingData, PreparedData]{

  override def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    val obs = trainingData.phraseAndSentiment
    val sqlContext = SQLContext.getOrCreate(sc)
    val phraseDataframe = sqlContext.createDataFrame(obs).toDF("phrase", "sentiment")
    val tf = processPhrase(phraseDataframe)
    val labeledpoints = tf.map(row => new LabeledPoint(row.getAs[Double]("sentiment"), row.getAs[Vector]("rowFeatures")))
    PreparedData(labeledpoints)
  }

  def processPhrase(phraseDataframe:DataFrame): DataFrame ={

    val tokenizer = new Tokenizer_new().setInputCol("phrase").setOutputCol("unigram")
    val unigram = tokenizer.transform(phraseDataframe)
    unigram.select("unigram").show(3,false)

    val ngram = new Ngram_new().setInputCol("unigram").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(unigram)
    ngramDataFrame.select("ngrams").show(3,false)

    val remover = new StopWordsRemover().setInputCol("ngrams").setOutputCol("filtered")
    val stopRemoveDF = remover.transform(ngramDataFrame)
    stopRemoveDF.select("filtered").show(3)

    var htf = new HashingTF().setInputCol("filtered").setOutputCol("rowFeatures")
    val tf = htf.transform(stopRemoveDF)
    tf.select("rowFeatures").show(3)

    tf
  }
}

case class PreparedData(var labeledpoints:RDD[LabeledPoint]) extends Serializable{
}

class Tokenizer_new extends Tokenizer(){

  override def createTransformFunc: (String) => Seq[String] = { str =>
    val NEGATION = "never|no|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|ain|aint|n't"
    val unigram = str.replaceAll("(?=.*\\w)^(\\w|')+$", "").replaceAll("[.*|!*|?*]","").replaceAll("((www\\.[^\\s]+)|(https?://[^\\s]+)|(http?://[^\\s]+))","")
      .replaceAll("(0-9*)|(0-9)+(a-z)*(.*|:*)","").toLowerCase().split("\\s+")
    (1 until unigram.length).foreach(i=>
      if(unigram(i-1).matches(NEGATION)){
        for(x <- i to unigram.length-1)
        {
          unigram(x) += "_NEG"
        }
      }
      else{
        unigram(i-1)
      }
    )
    unigram.toSeq
  }
}

class Ngram_new extends NGram(){

  override def createTransformFunc: (Seq[String]) => Seq[String] = { r =>
    val bigram = (1 until(r.length)).map(
      i => r(i-1) ++" "++ r(i)
    )
    val ngram = r ++ bigram
    ngram
  }

}




//    extra code --
//    val newRDD = obs.map(x => (x.phrase,x.sentiment))
//    val bigram = phraseDataframe.map(r => r.getAs[String](0).replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+").sliding(2,1)).flatMap{identity}.map{_.mkString(" ")}
//    val tokenizer = new Tokenizer().setInputCol("phrase").setOutputCol("words")
//    val tokenizedDF = tokenizer.transform(df)
//
//    val dff = tokenizedDF.select("words").collect()
//    val ans = dff.filter(x => x != " ")
//
//    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
//    val stopRemoveDF = remover.transform(tokenizedDF)
//
//    val ngram = new NGram().setN(1).setInputCol("filtered").setOutputCol("ngrams")
//
//    val ngramDataFrame = ngram.transform(stopRemoveDF)
//    val NEGATION_RE = """(?: ^(?:never|no|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|ain|aint)$) | n't """ .r
//    def mark_negation(ary: Seq[String]) : Seq[String] = {
//      val NEGATION = "never|no|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|ain|aint|n't"
//      println("negation called")
//      (1 until ary.length).foreach(i=>
//      if(ary(i-1).matches("not|never")){
//      for(x <- i to ary.length-1)
//    {
//      //println(x,ary(x),ary.length)
//      ary(x).concat("_NEG")
//    }
//
//    }
//      else{
//      println("no negation  --")
//      ary(i-1)
//    }
//      )
//      ary
//    }
//
//      def ngrams(ary: Seq[String]):Seq[String] ={
//      println("ngrams called")
//      val newary = (1 until(ary.length)).map(
//      i => ary(i-1) ++" "++ ary(i)
//      )
//      val merge = ary ++ newary
//      merge
//    }
