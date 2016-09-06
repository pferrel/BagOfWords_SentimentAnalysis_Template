package com.coviam.sentimentAnalysis.engine

/**
  * Created by bansarishah on 8/29/16.
  */
import org.apache.predictionio.controller.{Engine, EngineFactory, IEngineFactory}


object SA_EngineFactory extends EngineFactory{

  println("engine called ---")
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[DataPreparator],
      Map("Naivebayes" -> classOf[Algorithm]),
      classOf[Serving]
    )
  }
}