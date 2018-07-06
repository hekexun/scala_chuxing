package com.catc.test

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.Duration

import breeze.linalg.{DenseMatrix, diff}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import scala.collection.mutable.ArrayBuffer;

//读取hdfs里的文件
object hfile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\bigData\\hadoop-common-2.6.0-bin-master")
    val sconf = new SparkConf().setAppName("HelloHbase").setMaster("local")//("spark://192.168.80.3:7077").setJars(List("G:\\IDEA_Project\\catc_bigdate\\out\\artifacts\\catc_bigdate_jar\\catc_bigdate.jar"));
    val sc = new SparkContext(sconf);
   // val sqlContext = new SQLContext(sc);
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hbase
/*
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","cdh102,cdh103,cdh104,cdh105,cdh106,cdh107")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.rootdir",hdfs://cdh100/hbase)

    val tablename = "Dat_dbc"
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    val hBaseRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    def catalog = s"""{
                     }""".stripMargin;
    val df = withCatalog(catalog)*/
    /*
    //hive
    val rdd = sqlContext.sql("select * from h_datawarehouse t where t.ter in('987566464','987566435','987566463','987566485','987566520', '987566436','987566437','987566614','987566643','987566644','987566615','987566649','987566650','987566651','987566616', '987566617', '987566618',    '987566652',    '987566619',    '987567373',    '987568101',    '987568103', '987568895',    '987568888',    '987568900',    '987568903',    '987568889',    '987568887',    '987568893',    '987568902','987569619',    '987569620',    '987569621',    '987569851',    '987571018',    '987571019',    '987571363',    '987571404', '987571438',    '987571568',    '987571569',    '987571255',    '987571214',    '987571464', '987571439') and t.time >'20161010154642000' and t.time<'20161110154642000'").map(
     x=>{(x.toString().split("\t").apply(4),x.toString().split("\t").apply(2))});
    val rdd2=rdd.map(t=>{(JSONObject.fromObject(t._1),t._2)}).map(z=>{
      val data0=z._1.get("Data").toString().split(",").apply(6);
      //.toArray.apply(6);
      var engine=0.0;
      if ("null".equals(data0))
      {
        engine=0.0;
      }else
      {
        engine=data0.toString().toDouble;
      }

      (JSONObject.fromObject(z._1.get("EXT")).get("TerminalCode").toString().toDouble,
        JSONObject.fromObject(z._1.get("EXT")).get("TravelTime").toString(),
        JSONObject.fromObject(z._1.get("EXT")).get("GPSCarSpeed").toString().toDouble,
        engine
      )
    })hive结束*/

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的

    //本地
    val rdd=sc.textFile("file:///D:/data/上汽/shenzhen")//("hdfs://192.168.80.3:9000/txtfile2")
      .map(x=>{(x.split("\u0001").apply(4),x.split("\u0001").apply(2))});
    val rdd2=rdd.map(t=>{(JSONObject.fromObject(t._1),t._2)}).map(z=>{
     val data0=z._1.get("Data").toString().split(",").apply(6);
        //.toArray.apply(6);
      var engine=0.0;
      if ("null".equals(data0))
        {
           engine=0.0;
        }else
        {
          engine=data0.toString().toDouble;
        }

      (JSONObject.fromObject(z._1.get("EXT")).get("TerminalCode").toString().toDouble,
        JSONObject.fromObject(z._1.get("EXT")).get("TravelTime").toString(),
        JSONObject.fromObject(z._1.get("EXT")).get("GPSCarSpeed").toString().toDouble,
        engine
      )
      })
    val rdd_group =rdd2.groupBy(r=>r._1).sortBy(z=>{
      val sdf:SimpleDateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//***************优化的点
     var a= z._2.map(x=>sdf.parse(x._2.toString));
      a;
    }).map(zz=>{
      val time=zz._2.toArray;//转换为arrary
      val sd:SimpleDateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      var i=0;
      val re: ArrayBuffer[Array[String]]=new ArrayBuffer[Array[String]];
      var sum_num=0.0;
      var sum_mile=0.0;
      var startTime="";
      for(arr <-time)
        {
          if (i>0)
            {
              val d= (sd.parse( arr._2).getTime()-sd.parse(time.apply(i-1)._2).getTime());
              if(d>1800000)//d为毫秒，要转换为秒1000
              {
                val d2= (sd.parse( startTime).getTime()-sd.parse(time.apply(i-1)._2).getTime())/1000;
                val ar:Array[String]=Array(arr._1.toString,startTime,time.apply(i-1)._2,d2.toString,sum_num.toString,sum_mile.toString);
                re.append(ar);
                startTime=arr._2;
                sum_num=0;
                sum_mile=0;
              }else
              {
                sum_mile=sum_mile+arr._3/3600;
                sum_num=sum_num+1;
              }
            }
          else
            {
              startTime=time.apply(0)._2;
            }
          i=i+1;
        }
      (re);
    })

    val writer = new PrintWriter(new File("D:\\data\\上汽\\shenzhen.csv" ))
    rdd_group.collect.foreach(A=>A.foreach ( B =>{
      B.foreach(C => writer.write(C + ","));
        writer.write("\n");
    }))
    writer.close();
    sc.stop();
  }
}
