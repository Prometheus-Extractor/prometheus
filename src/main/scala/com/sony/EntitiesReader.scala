package com.sony.factextractor


case class Entity(id: String, name: Option[String] = None)
case class Relation(id: String, name: String, Seq[Tuple2[Entity]])

object EntitiesReader {
  def readEntities(file: String): Map[Relation, Seq[Tuple2[Entity]] = {
  }

}


