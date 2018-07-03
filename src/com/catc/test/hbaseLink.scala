/*
package com.catc.test
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext};

object hbaseLink {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\share\\hadoop-2.7.5\\hadoop-2.7.5")
    val sconf = new SparkConf().setAppName("HelloHbase").setMaster("spark://master:7077").setJars(List("G:\\IDEA_Project\\spark1215\\out\\artifacts\\spark1215_jar\\spark1215.jar"));
    val sc = new SparkContext(sconf);
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "master");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.rpc.timeout", "3600000")
    conf.set("hbase.client.scanner.timeout.period", "3600000")
    val tablename = "acount"
    val admin = new HBaseAdmin(conf);
    val htable = new HTable(admin.getConfiguration(), tablename);
    //1\确认是否存在该表
    if (admin.tableExists(tablename)) {
      //置为不可用,然后删除
      admin.disableTable(tablename);
      admin.deleteTable(tablename);
    }
    try {
      //2\创建描述
      val h_table = new HTableDescriptor(TableName.valueOf(tablename));
      val h_clomun = new HColumnDescriptor("dep_info");
      h_clomun.setBlocksize(64 * 1024);
      h_clomun.setBlockCacheEnabled(true);
      h_clomun.setMaxVersions(2); //最大版本号
      //添加到family
      h_table.addFamily(h_clomun);
      h_table.addFamily(new HColumnDescriptor("son_id".getBytes()));
      //3\创建表
      admin.createTable(h_table);

      ///插入数据


      //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
      val jobConf = new JobConf(conf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

      val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))


      val rdd = indataRDD.map(_.split(',')).map{arr=>{
        /*一个Put对象就是一行记录，在构造方法中指定主键
         * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
         * Put.add方法接收三个参数：列族，列名，数据
         */
        val put = new Put(Bytes.toBytes(arr(0).toInt))
        put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
        put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
        (new ImmutableBytesWritable, put)
      }}

      rdd.saveAsHadoopDataset(jobConf)
      //插入数据结束
      //读取数据
      // 如果表不存在则创建表

      if (!admin.isTableAvailable(tablename)) {
        val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
        admin.createTable(tableDesc)
      }

      //读取数据并转化成rdd
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val count = hBaseRDD.count()
      println(count)
      hBaseRDD.foreach{case (_,result) =>{
        //获取行键
        val key = Bytes.toString(result.getRow)
        //通过列族和列名获取列
        val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
        val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
        println("Row key:"+key+" Name:"+name+" Age:"+age)
      }}

      sc.stop()
      admin.close()
      //读取结束
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      admin.close();
    }

    sc.stop()
  }
}
*/