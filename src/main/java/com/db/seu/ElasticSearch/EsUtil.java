package com.db.seu.ElasticSearch;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.UnknownNamedObjectException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsUtil {
	 public static TransportClient client = null;  
	  
     /** 
      * 获取客户端 
      * @return 
      */  
     @SuppressWarnings("resource")
	public static TransportClient getClient() {  
         if(client!=null){  
             return client;  
         }  
         Settings settings = Settings.builder().put("cluster.name", "ubuntu").build();  
         try {  
             client = new PreBuiltTransportClient(settings) 
                     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ubuntu1"), 9300))  
                     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ubuntu3"), 9300));  
         } catch (UnknownNamedObjectException | UnknownHostException e) {  
             e.printStackTrace();  
         }  
         return client;  
     }  
     
     //添加数据即添加索引
     public static String addIndex(String index, String type, HashMap<String,Object> hashMap,String id){   
         IndexResponse response = getClient().prepareIndex(index, type, id).setSource(hashMap).get();  
         return response.getId();  
     } 
     
     //添加数据即添加索引
     public static String addIndex(String index, String type, HashMap<String,Object> hashMap){   
         IndexResponse response = getClient().prepareIndex(index, type).setSource(hashMap).get();  
         return response.getId();  
     } 
     
     //根据_id来获取一条document即记录
     public static void get(){
    	 GetResponse response = client.prepareGet(HbaseConfig.index, HbaseConfig.type, "54371").get();
    	 System.out.println(response.toString()+response.getId());
    	 for(String s : response.getFields().keySet()){
    		 System.out.println(s);
    	 }
     }
     
     //搜索所有的ducument,需要索引名称和类型
     public static void findAll() throws UnknownHostException {
         SearchResponse rs = client.prepareSearch(HbaseConfig.index).setTypes(HbaseConfig.type)
                 .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                 .setExplain(true).execute().actionGet();
         System.out.println(rs.getHits().getTotalHits());
         // 遍历查询结果
         for (SearchHit hit : rs.getHits().getHits()){
             System.out.println(hit.getId());
         }
     }
     
     //删除索引
     public static void delete(){
    	 DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(HbaseConfig.index)
                 .execute().actionGet();
    	 System.out.println(dResponse.isAcknowledged());
     }
     
     
     //测试两个条件检索,输入field即column和value
     public static void searchfilter(String column,String value,String c2, String v2){
    	 
    	 SearchResponse response = client.prepareSearch(HbaseConfig.index)
    		       .setTypes(HbaseConfig.type)
    		       .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    		       .setQuery(QueryBuilders.termQuery(column, value))
    		       .setPostFilter(QueryBuilders.termQuery(c2, v2))
    		       .setSize(100)
    		       .setExplain(true)
    		       .get();
    		SearchHits shs = response.getHits();
    		//根据查询到的rowkeys构建Get
    		List<Get> gets = new ArrayList<Get>();
    		System.out.println("num:" + shs.getTotalHits());
    		for(SearchHit hit : shs){
//    			String rowkey = (String)hit.getSource().get("rowKey");
    			String rowkey = hit.getId();
    			System.out.println(rowkey);
    			Get get = new Get(Bytes.toBytes(rowkey));
        		gets.add(get);

    		}
    		Result[] rs = null;
			try {
				rs = HbaseUtil.getInstance().getTable(HbaseConfig.tableName).get(gets);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            //打印hbase表查询结果
            for(Result r:rs){
                System.out.println(r); 
            }
     }
     
     
     
     //单个条件检索,输入field即column和value
     public static void search(String colum,String value){
    	 
    	 SearchResponse response = client.prepareSearch(HbaseConfig.index)
    		       .setTypes(HbaseConfig.type)
    		       .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    		       .setQuery(QueryBuilders.termQuery(colum, value))
    		       .setSize(100)
    		       .setExplain(true)
    		       .get();
    		SearchHits shs = response.getHits();
    		//根据查询到的rowkeys构建Get
    		List<Get> gets = new ArrayList<Get>();
    		System.out.println("num:" + shs.getTotalHits());
    		for(SearchHit hit : shs){
//    			String rowkey = (String)hit.getSource().get("rowKey");
    			String rowkey = hit.getId();
    			System.out.println(rowkey);
    			Get get = new Get(Bytes.toBytes(rowkey));
        		gets.add(get);

    		}
    		Result[] rs = null;
			try {
				rs = HbaseUtil.getInstance().getTable(HbaseConfig.tableName).get(gets);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            //打印hbase表查询结果
            for(Result r:rs){
                System.out.println(r); 
            }
     }
     
   //多个条件“与”检索,输入field即column和value
     public static void andSearch(String column1,String value1, String column2, String value2){
    	 QueryBuilder qb = QueryBuilders.boolQuery()
    			 .must(QueryBuilders.termQuery(column1, value1))
    			 .must(QueryBuilders.termQuery(column2, value2));
    	 
    	 SearchResponse response = client.prepareSearch(HbaseConfig.index)
    		       .setTypes(HbaseConfig.type)
    		       .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    		       .setQuery(qb)
    		       .setSize(100)
    		       .get();
    		SearchHits shs = response.getHits();
    		System.out.println("num:" + shs.getTotalHits());

    		//根据查询到的rowkeys构建Get
    		List<Get> gets = new ArrayList<Get>();
    		for(SearchHit hit : shs){
//    			String rowkey = (String)hit.getSource().get("_id");
    			String rowkey = hit.getId();
    			System.out.println(rowkey);
    			Get get = new Get(Bytes.toBytes(rowkey));
        		gets.add(get);

    		}
    		Result[] rs = null;
			try {
				rs = HbaseUtil.getInstance().getTable(HbaseConfig.tableName).get(gets);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            //打印hbase表查询结果
            for(Result r:rs){
                System.out.println(r); 
            }
     }
     
   //多个条件"或"检索,输入field即column和value
     public static void multiSearch(String column1, String value1, String column2, String value2){
    	 SearchRequestBuilder srb1 = client.prepareSearch(HbaseConfig.index)
							  		       .setTypes(HbaseConfig.type)
							  		       .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
							  		       .setSize(100)
							  		       .setQuery(QueryBuilders.termQuery(column1, value1));
    	 
    	 SearchRequestBuilder srb2 = client.prepareSearch(HbaseConfig.index)
	  		       .setTypes(HbaseConfig.type)
	  		       .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
	  		       .setSize(100)
	  		       .setQuery(QueryBuilders.termQuery(column2, value2));

    	 
    	 MultiSearchResponse responses = client.prepareMultiSearch()
    			 						 .add(srb1)
    			 						 .add(srb2)
    			 						 .get();
    	 for(MultiSearchResponse.Item item : responses.getResponses()){
    		 SearchResponse response = item.getResponse();
    		 SearchHits shs = response.getHits();
     		//根据查询到的rowkeys构建Get
     		List<Get> gets = new ArrayList<Get>();
     		for(SearchHit hit : shs){
//     			String rowkey = (String)hit.getSource().get("_id");
     			String rowkey = hit.getId();
     			System.out.println(rowkey);
     			Get get = new Get(Bytes.toBytes(rowkey));
         		gets.add(get);

     		}
     		Result[] rs = null;
 			try {
 				rs = HbaseUtil.getInstance().getTable(HbaseConfig.tableName).get(gets);
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			} 
             //打印hbase表查询结果
             for(Result r:rs){
                 System.out.println(r); 
             }
    	 }
    		
     }
     public static void ClientClose(){
    	 client.close();
     }
}
