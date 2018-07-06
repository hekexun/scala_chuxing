package com.catc.test

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import net.sf.json.JSONObject
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer;

//读取hdfs里的文件
object hfilesegment {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\share\\hadoop-2.7.5\\hadoop-2.7.5")
    val sconf = new SparkConf().setAppName("HelloHbase").setMaster("local")//("spark://192.168.80.3:7077").setJars(List("G:\\IDEA_Project\\catc_bigdate\\out\\artifacts\\catc_bigdate_jar\\catc_bigdate.jar"));
    val sc = new SparkContext(sconf);
    val sqlContext = new SQLContext(sc);
    val rdd=sc.textFile("file:\\\F:\上汽1\上汽1\chongqing\0*")//("hdfs://192.168.80.3:9000/txtfile2")
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
    val rdd_group =rdd2.groupBy(r=>r._1).sortBy(z=>{
      val sdf:SimpleDateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//***************优化的点
     var a= z._2.map(x=>sdf.parse(x._2.toString));
      a;
    }).flatMap(zz=>{
      val time=zz._2.toArray;//转换为arrary
      val sd:SimpleDateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      var i=0;
      val re: ArrayBuffer[Array[String]]=new ArrayBuffer[Array[String]];
      var run_begin="";
      var run_end="";
      var idl_begin="";
      var startTime="";
      var idl_num=0;
      val vseq:ArrayBuffer[String]=new ArrayBuffer[String]();
      var flag=0;//等于0表示0,如果从0变道1，表示一个片段结束了，开始新的片段。
      for(arr <-time)
        {
          if (i>0)
            {
              val d= (sd.parse( arr._2).getTime()-sd.parse(time.apply(i-1)._2).getTime());
              if(arr._3.toDouble>0.5&flag==0)//flag=0.表示上一个点的值为0，这个点的值>0,开始新的片段
              {
                var td=100;
                if(startTime.length>1)
                  {
                    val td= (sd.parse(time.apply(i-1)._2).getTime()-sd.parse(startTime).getTime());
                  }
                val ar:Array[String]=Array(arr._1.toString,startTime,idl_begin,time.apply(i-1)._2,"","","","","",td.toString,idl_num.toString,vseq.mkString("[", ",", "]"));
                    re.append(ar.toArray)
                    startTime=arr._2;
                    flag=1;
                    idl_num=0;

              }else if (d>120000&flag==0)//两点间隔>2分钟，且速度=0.表示要切割
              {
                var td=100.00;
                if(startTime.length>1)
                {
                   td= (sd.parse(time.apply(i-1)._2).getTime()-sd.parse(startTime).getTime());
                }
                val ar:Array[String]=Array(arr._1.toString,startTime,idl_begin,time.apply(i-1)._2,"","","","","",td.toString,idl_num.toString,vseq.mkString("[", ",", "]"));
                re.append(ar.toArray);
                idl_num=0;
              }
              else if(arr._3<0.5&flag==1)//速度=0，上一点速度>0;开始进入怠速时间
              {
                vseq.append(arr._3.toString);
                idl_begin=arr._2;
                flag=0;
              }
              else if(arr._3<0.5&idl_num>199){//怠速超过199，停止

                var td=100;
                if(startTime.length>1)
                {
                  val td= (sd.parse(time.apply(i-1)._2).getTime()-sd.parse(startTime).getTime());
                }
                val ar:Array[String]=Array(arr._1.toString,startTime,idl_begin,time.apply(i-1)._2,"","","","","",td.toString,idl_num.toString,vseq.mkString("[", ",", "]"));
                re.append(ar.toArray);
                flag=0;
                idl_num=0;
              }else if(arr._3>0.5&flag==1)
                {

                  vseq.append(arr._3.toString);
                }else if(arr._3<0.5&flag==0)
              {
                    vseq.append(arr._3.toString);
                    idl_num=idl_num+1;
              }

            }
          i=i+1;
        }
      re;
    })

    val writer = new PrintWriter(new File("D:\\data\\chongqing.csv" ))
    rdd_group.collect.foreach(A=>A.foreach ( B =>{
      B.foreach(C => writer.write(C + ","));
      writer.write("\n");
    }))
    writer.close();
    sc.stop();
  }
  def segment(arr:ArrayBuffer[Array[String]])=
  {

  }
}
