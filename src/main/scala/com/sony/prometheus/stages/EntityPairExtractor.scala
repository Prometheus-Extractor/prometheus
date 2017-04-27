package com.sony.prometheus.stages

import com.sony.prometheus.utils.Utils.pathExists
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable

class RelationConfigData(path: String)(implicit sc: SparkContext) extends Data{
  override def getData(): String = {
    if (pathExists(path)) {
      path
    } else {
      throw new Exception(s"Relation config data missing $path")
    }
  }
}

class WikidataData(path: String)(implicit sc: SparkContext) extends Data{
  override def getData(): String = {
    if (pathExists(path)) {
      path
    } else {
      throw new Exception(s"Wikidata data missing $path")
    }
  }
}

class EntityPairExtractorStage(path: String, configData: RelationConfigData, wikidata: WikidataData)
                              (implicit sqlContext:SQLContext, sparkContext: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    val relationList = RelationConfigReader.load(configData.getData())
    // Each relation yields about 100kb parquet data
    val data = EntityPairExtractor.getEntities(wikidata.getData(), relationList, "en", sqlContext, sparkContext).repartition(1)

    import sqlContext.implicits._
    val df = data.map(relation => {

      val entities = relation._2.map(entTuple => {
        EntityPair(entTuple._1, entTuple._2)
      }).toSeq

      val r = relationList.find(_.id == relation._1).get
      r.copy(entities = entities)
    }).toDF()

    df.write.parquet(path)

  }

}

object RelationConfigReader {

  def load(path: String)(implicit sqlContext: SQLContext): Seq[Relation] = {
    sqlContext.read.text(path).rdd.zipWithIndex().map{
      case (row, index) =>
        val cols = row.getString(0).split("\t")
        val types = for {
          type1 <- cols.lift(2)
          type2 <- cols.lift(3)
        } yield (type1, type2)
        Relation(cols(0), cols(1), index.toInt + 1, types)
    }.collect()
  }

}

object EntityPairExtractor {

  def load(file: String)(implicit sqlContext: SQLContext): RDD[Relation] = {
    import sqlContext.implicits._
    sqlContext.read.parquet(file).as[Relation].rdd
  }

  def getEntities(filePath:String,
                  relations: Seq[Relation],
                  lang: String,
                  sqlContext:SQLContext,
                  sc:SparkContext): RDD[(String, Iterable[(String, String)])] = {

    val targetRelations: Broadcast[Set[String]] = sc.broadcast(relations.map(x => x.id).toSet[String])

    val wd = sqlContext.read.parquet(filePath)
    val wdTyped: RDD[WdEntity] = wd.map(convert(_, lang))

    val entities = wdTyped.flatMap(entity => {

      entity.properties.filter(p => targetRelations.value.contains(p.key)).map{prop: WdProperty =>
        (prop.key -> (entity.id -> prop.value))
      }.groupBy(_._1).map(kv => kv._1 -> kv._2.map(x=> x._2).sorted.distinct)

    }).groupByKey().map(kv => kv._1 -> kv._2.flatten)

    targetRelations.destroy()
    entities
  }

  def clean(str : String): String = str.trim.replaceAll("[\1\\|\\t\\n\\r\\p{Z}]"," ")

  def convert(row: Row, lang: String): WdEntity = {
    /* Code compliments of Marcus Klang */
    val id = row.getString(0)
    val description = row.getSeq[Row](1)
      .map(row => (row.getString(0), clean(row.getString(1))))
      .find({ case (dlang, desc) => dlang == lang })
      .getOrElse((lang, ""))._2

    val title = row.getSeq[Row](5)
      .map(row => (row.getString(0), clean(row.getString(1))))
      .find({ case (dlang, lbl) => dlang == lang })
      .getOrElse(lang, "")
      ._2

    val alias = row.getSeq[Row](4)
      .map(row => (row.getString(0), clean(row.getString(1))))
      .filter({ case (dlang, lbl) => dlang == lang })
      .map({case(dlang,lbl) => lbl})
      .toArray

    val sitelink = row.getSeq[Row](3)
      .map(row => (row.getString(0), clean(row.getString(1))))
      .find({ case (dlang, lbl) => dlang == lang })
      .getOrElse(lang, "")
      ._2

    val parentproperties = new mutable.HashMap[Int, WdProperty]() ++ row.getSeq[Row](2).flatMap(lrow => {
      if (lrow.isNullAt(4)) {
        List((lrow.getInt(0), WdProperty(lrow.getString(1), clean(lrow.getString(2)), lrow.getString(3), null)))
      }
      else
        List()
    })

    row.getSeq[Row](2)
      .flatMap(lrow => {
        if (!lrow.isNullAt(4) && parentproperties.contains(lrow.getInt(4))) {
          val parent = parentproperties(lrow.getInt(4))
          List((lrow.getInt(4), WdProperty(lrow.getString(1), clean(lrow.getString(2)), lrow.getString(3), null)))
        }
        else
          List()
      }).groupBy({ case (parentid, prop) => parentid })
      .foreach({ case (parentid, children) => parentproperties(parentid).children = children.map({ case (parentid_int, prop) => prop }).toArray })

    WdEntity(id, description, alias, title, sitelink, parentproperties.values.toArray)
  }


}

case class WdProperty(key : String, value : String, datatype : String, var children : Array[WdProperty])
case class WdEntity(id : String, description : String, alias : Array[String], label : String, sitelink : String, properties : Array[WdProperty])

case class Relation(name: String, id: String, classIdx: Int, types:Option[(String, String)], entities: Seq[EntityPair] = Seq())
case class EntityPair(source: String, dest: String)
