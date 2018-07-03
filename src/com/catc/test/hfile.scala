package com.catc.test

import java.text.SimpleDateFormat
import java.time.Duration

import breeze.linalg.{DenseMatrix, diff}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import net.sf.json.JSONObject

import scala.collection.mutable.ArrayBuffer;

//读取hdfs里的文件
object hfile {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\share\\hadoop-2.7.5\\hadoop-2.7.5")
    val sconf = new SparkConf().setAppName("HelloHbase").setMaster("local")//("spark://192.168.80.3:7077").setJars(List("G:\\IDEA_Project\\catc_bigdate\\out\\artifacts\\catc_bigdate_jar\\catc_bigdate.jar"));
    val sc = new SparkContext(sconf);
    val sqlContext = new SQLContext(sc);
    val rdd=sc.textFile("F:\\share\\000039_0")//("hdfs://192.168.80.3:9000/txtfile2")
      .map(x=>{(x.split("\t").apply(4),x.split("\t").apply(2))});
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
    /*sparksql案例
     val df=sqlContext.createDataFrame(rdd2);  // 生成一个dataframe
     val df_name_colums=df.toDF("te","time","speed","engine")  //给df的每个列取名字
    df_name_colums.registerTempTable("carTable")     //注册临时表
    val sql="select te,time,speed,engine from carTable where engine>0"
    val rs: DataFrame =sqlContext.sql(sql);
    rs.foreach(x=>println(x.toString()));
    val rs_rdd=rs.rdd.map(x=>(x(0),x(1)));
    //rs_rdd.saveAsTextFile("/home/wangtuntun/test_file5.txt");
    */
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
                val ar:Array[String]=Array(arr._1.toString,startTime,time.apply(i-1)._2,sum_num.toString,sum_mile.toString);
                re.append(ar);
                startTime=arr._2;
                sum_num=0;
                sum_mile=0;
              }else
              {
                sum_mile=sum_mile+arr._1;
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
    rdd_group.saveAsTextFile("F:\\share\\000039_02")
    sc.stop();
  }
}
