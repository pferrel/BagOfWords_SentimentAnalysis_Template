package com.coviam.sentimentAnalysis.engine

import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{Tokenizer, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


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

    val ngram = new Ngram_new().setInputCol("unigram").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(unigram)

    val remover = new StopWordsRemover().setInputCol("ngrams").setOutputCol("filtered")
    val stopRemoveDF = remover.transform(ngramDataFrame)

    var htf = new HashingTF().setInputCol("filtered").setOutputCol("rowFeatures")
    val tf = htf.transform(stopRemoveDF)

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




