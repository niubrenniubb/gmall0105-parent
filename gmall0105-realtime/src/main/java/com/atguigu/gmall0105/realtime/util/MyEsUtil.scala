package com.atguigu.gmall0105.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder



object MyEsUtil {

  var factory:JestClientFactory=null;

  def getClient:JestClient={
    if(factory==null)build()
    factory.getObject
  }

  def build():Unit={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://192.168.206.127:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000).readTimeout(10000).build())
  }

  def addDoc():Unit={
    val jest:JestClient=getClient
    val index = new Index.Builder(movie("meiwenti","henwudi",null)).index("movie_20200907").`type`("_doc").id("0104").build()
    val message=jest.execute(index).getErrorMessage
    if(message!=null){
      println(message)
    }
    jest.close()
  }

  def queryDoc():Unit={
    val jest:JestClient=getClient

    val searchsourcebuilder= new SearchSourceBuilder()
    val boolQueryBuilder=new BoolQueryBuilder();
    boolQueryBuilder.filter(new TermQueryBuilder("moive_name.keyword","meiwenti"))
    searchsourcebuilder.query(boolQueryBuilder)
    val query2:String=searchsourcebuilder.toString
    println(query2)
    val search=new Search.Builder(query2).addIndex("movie_20200907").addType("_doc").build()
    val result:SearchResult=jest.execute(search)
    val hits:util.List[SearchResult#Hit[util.Map[String,Any],Void]]=result.getHits(classOf[util.Map[String,Any]])
    import scala.collection.JavaConversions._
    for(hit <- hits){
      println(hit.source.mkString("\n"))
    }
    jest.close()
  }



  def bulkDoc(sourceList:List[Any],indexName:String):Unit={
    if(sourceList!=null&&sourceList.size>0) {
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for (source <- sourceList) {
        val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      val items: java.util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES:" + items.size() + "条数")
      jest.close()
    }
  }



  def main(args: Array[String]): Unit = {
    queryDoc()
  }

  case class movie(moive_name:String,name: String,ActorList:Array[String])


}
