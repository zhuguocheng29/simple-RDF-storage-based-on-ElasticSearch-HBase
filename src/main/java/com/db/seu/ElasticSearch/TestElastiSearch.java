package com.db.seu.ElasticSearch;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTableInterface;


/**
 * Hello world!
 *
 */
public class TestElastiSearch 
{
    public static void main( String[] args )
    {
		String tableName = HbaseConfig.tableName;
		try {
			if (!HbaseUtil.getAdmin().tableExists(tableName)) {
				String[] families = { "objects", "attributes", "array_objects"};
				HbaseUtil.createTable(tableName, families);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		HTableInterface hTable = null;
		try {
			hTable = HbaseUtil.getInstance().getTable(tableName);
			EsUtil.getClient();
			//插入数据
			HbaseUtil.getInstance().putTable();
			HbaseUtil.getInstance().addOneRecord();
			HbaseUtil.getInstance().addTwoJoin();
			//删除index
			//EsUtil.delete();
			//根据id获得记录
//			EsUtil.get();
			HbaseUtil.getInstance().getTable(HbaseConfig.tableName);
			long start = System.currentTimeMillis();
//			EsUtil.findAll();
			//单个条件查询
			EsUtil.search("className","zhu0");
//			EsUtil.search("bdnm","wayne");
			//s-id:079546bdnm:967923 className:zhu148 bdnm:One
			//EsUtil.search("bdnm","967923");
			//两个条件查询
//			EsUtil.andSearch("className","zhu0","bdnm","wayne");
			
//			EsUtil.searchfilter("className","zhu148","bdnm","One");
			System.out.println(System.currentTimeMillis()-start);

		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
