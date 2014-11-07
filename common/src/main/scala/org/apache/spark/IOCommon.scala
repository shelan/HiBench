package org.apache.spark.SparkBench

import java.io.{InputStreamReader, IOException, FileInputStream, File}
import java.util.Properties

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.{SparkException, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap

class IOCommon(val sc:SparkContext) {
  def load[T:ClassTag:TypeTag](filename:String) = {
    val input_format:String = IOCommon.getProperty("sparkbench.inputformat").getOrElse("Text")
    input_format match {
      case "Text" =>
        sc.textFile(filename)

      //      case "Object" => sc.objectFile[T](filename)

      case "Sequence" =>
        sc.sequenceFile[NullWritable, Text](filename).map(_._2.toString)

      case _ => throw new UnsupportedOperationException(s"Unknown inpout format: $input_format")
    }
  }

  def save(filename:String, data:RDD[_], prefix:String = "sparkbench.outputformat") = {
    val output_format = IOCommon.getProperty(prefix).getOrElse("Text")
    val output_format_codec = loadClassByName[CompressionCodec](IOCommon.getProperty(prefix + ".codec"))

//    IOCommon.dumpProperties()
//    println(s"Using codec:$output_format_codec, key:${prefix}.codec")

    output_format match {
      case "Text" =>
        if (output_format_codec.isEmpty)  data.saveAsTextFile(filename)
        else data.saveAsTextFile(filename, output_format_codec.get)

      case "Sequence" =>
        val sequence_data = data.map(x => (NullWritable.get(), new Text(x.toString)))
        if (output_format_codec.isEmpty)
          sequence_data.saveAsHadoopFile[SequenceFileOutputFormat[NullWritable, Text]](filename)
        else
          sequence_data.saveAsHadoopFile[SequenceFileOutputFormat[NullWritable, Text]](filename,
            output_format_codec.get)

      case "Object" => data.saveAsObjectFile(filename)
      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
    }
  }

  //  def load_depracted[T:ClassTag:TypeTag](filename:String) = {
  //    val input_format = System.getProperty("sparkbench.inputformat", "Text")
  //    input_format match {
  //      case "Text" =>
  //        require(typeOf[T] <:< typeOf[String])
  //        sc.textFile(filename).asInstanceOf[RDD[T]]
  //
  //      case "Object" => sc.objectFile[T](filename)
  //
  //      case "Sequence" =>
  //        val input_key_cls = loadClassByName[Writable](System.getProperty("sparkbench.inputformat.key_class",
  //              "org.apache.hadoop.io.LongWritable"))
  //        val input_value_cls = loadClassByName[Writable](System.getProperty("sparkbench.inputformat.value_class",
  //              "org.apache.hadoop.io.LongWritable"))
  //        val input_value_cls_method = System.getProperty("sparkbench.inputformat.value_class_method", "get")
  //        sc.sequenceFile(filename, input_key_cls.getClass, input_value_cls.getClass)
  //               .map(x=>callMethod[Writable, T](x._2, input_value_cls_method))
  //
  //      case _ => throw new UnsupportedOperationException(s"Unknown inpout format: $input_format")
  //    }
  //  }
  //
  //  def save_depracted[T](filename:String, data:RDD[T], prefix:String = "sparkbench.outputformat") = {
  //    val output_format = System.getProperty(prefix, "Text")
  //    val output_format_codec = System.getProperty(prefix + ".codec",
  //      "org.apache.hadoop.io.compress.SnappyCodec")
  //    println(s"codec:$output_format_codec")
  //    output_format match {
  //      case "Text" =>
  //        if (output_format_codec == "None") data.saveAsTextFile(filename)
  //        else {
  //          data.saveAsTextFile(filename,
  //            loadClassByName[CompressionCodec](output_format_codec))
  //        }
  //      case "Sequence" =>
  //        data.map(x=> (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
  //        .saveAsSequenceFile(filename)
  //      case "Object" => data.saveAsObjectFile(filename)
  //      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
  //    }
  //  }

  private def loadClassByName[T](name:Option[String]) = {
    if (!name.isEmpty) Some(Class.forName(name.get).newInstance.asInstanceOf[T].getClass) else None
  }

  private def callMethod[T, R](obj:T, method_name:String) =
    obj.getClass.getMethod(method_name).invoke(obj).asInstanceOf[R]
}

object IOCommon{
  private val sparkbench_conf: HashMap[String, String] =
    getPropertiesFromFile(System.getenv("SPARKBENCH_PROPERTIES_FILES"))

  def getPropertiesFromFile(filenames: String): HashMap[String, String] = {
    val result = new HashMap[String, String]
    filenames.split(',').filter(_.stripMargin.length > 0).foreach { filename =>
      val file = new File(filename)
      require(file.exists, s"Properties file $file does not exist")
      require(file.isFile, s"Properties file $file is not a normal file")

      val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
      try {
        val properties = new Properties()
        properties.load(inReader)
        result ++= properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
      } catch {
        case e: IOException =>
          val message = s"Failed when loading Sparkbench properties file $file"
          throw new SparkException(message, e)
      } finally {
        inReader.close()
      }
    }
    result.filter{case (key, value) => value.toLowerCase != "none"}
  }

  def getProperty(key:String):Option[String] = sparkbench_conf.get(key)

  def dumpProperties(): Unit = sparkbench_conf.foreach{case (key, value)=> println(s"$key\t\t$value")}
}