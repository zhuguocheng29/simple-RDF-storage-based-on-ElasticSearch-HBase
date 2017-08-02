
package com.db.seu.ElasticSearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Description: a class in order to obtain hbabse configuration, htable and hbaseAdmin.
 * @author ttf
 */
public class HbaseUtil {

	private static HbaseUtil hbaseTool;
	private static HBaseAdmin hBaseAdmin;
	private static Configuration baseConfiguration;
	private HTableInterface htable;
	private HTableInterface indexTable = null;
	private static HConnection connection;
	private static List<HRegionInfo> hList;
	

	static {
		baseConfiguration = HBaseConfiguration.create();
		baseConfiguration.set("hbase.zookeeper.quorum",HbaseConfig.hostip);
		baseConfiguration.setInt("hbase.zookeeper.property.clientPort", HbaseConfig.port);
		try {
			connection = HConnectionManager.createConnection(baseConfiguration);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			hBaseAdmin = new HBaseAdmin(baseConfiguration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private HbaseUtil() {
	}

	public static HBaseAdmin getAdmin() {

		return hBaseAdmin;
	}

	public static Configuration getBaseConfiguration() {
		return baseConfiguration;
	}

	public static void setBaseConfiguration(Configuration baseConfiguration) {
		HbaseUtil.baseConfiguration = baseConfiguration;
	}

	public static HbaseUtil getInstance() {

		if (hbaseTool == null)
			hbaseTool = new HbaseUtil();

		return hbaseTool;
	}

	public HTableInterface getTable(String tableName) throws IOException {

		if (htable == null) {
			htable = connection.getTable(TableName.valueOf(tableName));
			htable.setWriteBufferSize(10*1024*1024);
			htable.setAutoFlushTo(false);
		}

		return htable;

	}
	
	/**
	 * 在建表时加载协处理器,增加了index这一个family
	 * @param tableName
	 */
	public static void createTable(String tableName,String[] families)
	{
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor[] colDesArray=new HColumnDescriptor[families.length];
		for(int i=0;i<families.length;i++){
			colDesArray[i]=new HColumnDescriptor(families[i]);
			colDesArray[i].setBloomFilterType(BloomType.ROW);
			colDesArray[i].setMaxVersions(1);
			hTableDescriptor.addFamily(colDesArray[i]);
		}

		//设10个region块，因为索引表与数据放在一起
		byte[][] regions = new byte[][]{
			Bytes.toBytes("0000"),
			Bytes.toBytes("1000"),
			Bytes.toBytes("2000"),
			Bytes.toBytes("3000"),
			Bytes.toBytes("4000"),
			Bytes.toBytes("5000"),
			Bytes.toBytes("6000"),
			Bytes.toBytes("7000"),
			Bytes.toBytes("8000"),
			Bytes.toBytes("9000"),
			Bytes.toBytes("9999a"),
		};
		
		try {
			hBaseAdmin.createTable(hTableDescriptor,regions);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("builtTable");

	}
	
	
	public String getRegionNumber(String id)
	{
		String regionNumber = "";
		switch(id)
		{
			case "0": 
					regionNumber = "0000";
					break;
			case "1": 
					regionNumber = "1000";
					break;
			case "2": 
					regionNumber = "2000";
					break;
			case "3": 
					regionNumber = "3000";
					break;
			case "4": 
					regionNumber = "4000";
					break;
			case "5": 
					regionNumber = "5000";
					break;
			case "6": 
					regionNumber = "6000";
					break;
			case "7": 
					regionNumber = "7000";
					break;
			case "8": 
					regionNumber = "8000";
					break;
			case "9": 
					regionNumber = "9000";
					break;
		}
		return regionNumber;
	}
	
	/**
	 * 随机获取前缀
	 * @return
	 */
	public String getRandomPrefix()
	{
		String base = "0123456789";   
	    Random random = new Random();   
	    StringBuffer sb = new StringBuffer();   
	    for (int randomNumber = 0; randomNumber < 4; randomNumber++) {   
	        int number = random.nextInt(base.length());   
	        sb.append(base.charAt(number));   
	    }   
	    String prefix = sb.toString();
	    return prefix;
	}
	
	/**
	 * 测试用
	 * @throws IOException
	 */
	public void putTable() throws IOException
	{
		System.out.println("successful read");
		for(int i=1; i<100000; i++)
		{
			String prefix = getRandomPrefix();
			String dataIndex = prefix+":test:"+prefix.hashCode()+":"+i;
			Put put = new Put(Bytes.toBytes(dataIndex));
			HashMap<String, Object> hashMap = new HashMap<String, Object>();
		    if(i%22 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("wayne"));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes((prefix+":"+1000)+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
				
				//建elasticsearch 索引
				hashMap.put("bdnm","wayne");
				hashMap.put("className", "zhu0");
//				hashMap.put("age", i+"");
				hashMap.put("parent", prefix+":"+1000+"");
				hashMap.put("car", "Ferrari");
//				EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hashMap);
		    }
		    else if(i%7 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("think"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
				//建elasticsearch 索引
//				HashMap<String, Object> hashMap = new HashMap<String, Object>();
				hashMap.put("bdnm","think");
				hashMap.put("className", "zhu" + i);
//				hashMap.put("age", i+"");
				hashMap.put("car", "Benz");
				hashMap.put("random", prefix);
//				EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hashMap);
		    }
		    else if(i%37 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("One"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
				//建elasticsearch 索引
//				HashMap<String, Object> hashMap = new HashMap<String, Object>();
				hashMap.put("bdnm","One");
				hashMap.put("className", "zhu" + i);
//				hashMap.put("age", i+"");
				hashMap.put("car", "Benz");
				hashMap.put("random", prefix);
//				EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hashMap);
		    }
		    else
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("s-id"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes("Null"));
				//建elasticsearch 索引
//				HashMap<String, Object> hashMap = new HashMap<String, Object>();
				hashMap.put("bdnm",(prefix+i)+"");
				hashMap.put("s-id", (prefix+i)+"");
//				hashMap.put((i+1)+"", "Null");
		    }

		    htable.put(put);
			htable.flushCommits();
			
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hashMap,dataIndex);


		}

		Put put = new Put(Bytes.toBytes("0111:test:-123:1"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
		
		//建elasticsearch 索引
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("data","zhu123");
		hashMap.put("className", "zhu0");
		
		htable.put(put);
		htable.flushCommits();
		EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hashMap,"0111:test:-123:1");

		System.out.println("successful insert");
		
	}
	
	/**
	 * 测试用，用于测试单个join,添加parent数据
	 */
	public void addOneRecord(){
		System.out.println("start");
		for(int i=1; i<10; i++)
		{
			String prefix = getRandomPrefix();
			String dataIndex = prefix+":test123:"+prefix.hashCode()+":"+i;
			
			Put put = new Put(Bytes.toBytes(dataIndex));	    	
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Rooney"));
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			String t = prefix.substring(0, 3)+"0:topic:"+prefix.hashCode()+":"+i;
			put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(t));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
			
			HashMap<String, Object> hash1 = new HashMap<String, Object>();
			hash1.put("bdnm","Rooney");
			hash1.put("className", "zhu0");
			hash1.put("car", "Ferrari");
			hash1.put("parent", t);
			try {
			    htable.put(put);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hash1,dataIndex);

			Put put1 = new Put(Bytes.toBytes(t));
	    	put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Man"));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
		   
			HashMap<String, Object> hash2 = new HashMap<String, Object>();
			hash2.put("bdnm","Man");
			hash2.put("className", "zhu" + i);
			hash2.put("car", "Benz");
		    try {
				htable.put(put1);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hash2,t);

		}

		System.out.println("end");
		
	}
	

	/**
	 * 测试两个Join
	 */
	public void addTwoJoin(){
		System.out.println("start");
		for(int i=1; i<10; i++)
		{
			String prefix = getRandomPrefix();
			String indexPrefixTemp = prefix.substring(0, 1);
			String dataIndex = prefix+":judge:"+prefix.hashCode()+":"+i;
			
			Put put = new Put(Bytes.toBytes(dataIndex));	    	
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Jack"));
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			String t = prefix.substring(0, 3)+"0:topic:"+prefix.hashCode()+":"+i;
			put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(t));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
			
			HashMap<String, Object> hash1 = new HashMap<String, Object>();
			hash1.put("bdnm","Jack");
			hash1.put("className", "zhu0");
			hash1.put("car", "Ferrari");
			hash1.put("parent", t);
			
			try {
			    htable.put(put);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hash1,dataIndex);

			Put put1 = new Put(Bytes.toBytes(t));
	    	put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Rose"));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
			String s = prefix.substring(0, 3)+"5:result:"+prefix.hashCode()+":"+i;
			put1.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(s));
		    
			HashMap<String, Object> hash2 = new HashMap<String, Object>();
			hash2.put("bdnm","Rose");
			hash2.put("className", "zhu" + i);
			hash2.put("car", "Benz");
			hash2.put("parent", s);
			
			try {
				htable.put(put1);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hash2,t);

			Put put2 = new Put(Bytes.toBytes(s));
	    	put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Baby"));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
		    
			HashMap<String, Object> hash3 = new HashMap<String, Object>();
			hash3.put("bdnm","Baby");
			hash3.put("className", "zhu" + i);
			hash3.put("car", "Benz");
			
			try {
				htable.put(put2);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			EsUtil.addIndex(HbaseConfig.index,HbaseConfig.type,hash3,s);


		}

		System.out.println("end");
		
	}

	
	/**
	 * close the connection.
	 */
	public void close() {
		try {
			htable.close();
			connection.close();
			hBaseAdmin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
