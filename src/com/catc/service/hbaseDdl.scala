package com.catc.service

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

 class hbaseDdl {
   //转换函数
   /*在 HBase 中的表 schema 一般是这样的：
     row     cf:col_1    cf:col_2
     而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14), (2,"hanmei",18)。
     我们需要将 RDD[(uid:Int, name:String, age:Int)] 转换成 RDD[(ImmutableBytesWritable, Put)]。
     所以，我们定义一个 convert 函数做这个转换工
    */
   def convert(triple: (Int, String, Int)) = {
     val p = new Put(Bytes.toBytes(triple._1));
     p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
     p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
     (new ImmutableBytesWritable, p)
   }
   //数据存储到hbase
  def  saveAsHadoopDataset(conf: JobConf,sc: SparkContext): Unit=
   {
     val conf = HBaseConfiguration.create()
     conf.set("hbase.zookeeper.property.clientPort", "2181")
     conf.set("hbase.zookeeper.quorum", "master")

     //指定输出格式和输出表名
     val jobConf = new JobConf(conf,this.getClass)
     jobConf.setOutputFormat(classOf[TableOutputFormat])
     jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")
     //read RDD data from somewhere and convert
     val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
     val localData = sc.parallelize(rawData).map(convert)
     localData.saveAsHadoopDataset(jobConf)

   }

}
