package testscala

import spark._
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.Counters
import org.apache.hadoop.mapred.Counters.Counter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Partitioner => HadoopPartitioner}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.StatusReporter
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.util.Progress
import org.apache.hadoop.conf.Configuration

import java.lang.reflect.Constructor
import java.io.DataOutputStream
import java.io.DataInputStream
import java.util.HashSet

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import testjava.WordCountJob


/**
 * This class takes a Hadoop Job object and runs it as a Spark job.
 */
 class SparkHadoopJob[
     MapInputKey <: Writable : ClassManifest, MapInputValue <: Writable : ClassManifest, 
     MapOutputKey <: Writable: ClassManifest, MapOutputValue <: Writable : ClassManifest, 
     ReduceOutputKey <: Writable : ClassManifest, ReduceOutputValue <: Writable : ClassManifest
   ] (@transient job: HadoopJob) extends Serializable {
  
  
  /**
   * This subclass is a wrapper around Hadoop Mapper (new API). It implements a custom Mapper.Context
   * that is given to the mapper object for processing the data.
   */
  class WrappedMapper() extends Serializable {
   
    def run(inputIterator: Iterator[(MapInputKey, MapInputValue)]): Iterator[(MapOutputKey, MapOutputValue)] = {
      val mapperClass: Class[Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue]] = 
        jobContext.getMapperClass().asInstanceOf[Class[Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue]]]        
      
      val mapper: Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue] = createNewInstance(mapperClass)
      
      val mapperContext = new mapper.Context(
          jobConf, new TaskAttemptID(), null, null, null, new WrappedReporter(), null) {
        var currentKeyValue: (MapInputKey, MapInputValue) = null
        val outputBuffer = new ArrayBuffer[(MapOutputKey, MapOutputValue)]() 

        override def nextKeyValue() = if (inputIterator.hasNext) {
          currentKeyValue = inputIterator.next()
          true
        } else {
          currentKeyValue = null
          false
        }      

        override def getCurrentKey(): MapInputKey = if (currentKeyValue == null) 
          null.asInstanceOf[MapInputKey] else currentKeyValue._1       

        override def getCurrentValue(): MapInputValue = if (currentKeyValue == null) 
          null.asInstanceOf[MapInputValue] else currentKeyValue._2

        override def write(key: MapOutputKey, value: MapOutputValue) { 
          outputBuffer += ((WritableUtils.clone(key, jobConf), value)) 
        }

        def outputIterator() = {
          println("# map output key-value pairs = " + outputBuffer.length)
          outputBuffer.toIterator
        }
      }
      
      mapper.run(mapperContext)
      mapperContext.outputIterator()       
    }
  }
  
  /**
   * This subclass is a wrapper around Hadoop Reducer (new API). It implements a custom Reducer.Context
   * that is given to the mapper object for processing the data.
   */
  class WrappedReducer() extends Serializable {
    
    def run(iterator: Iterator[(MapOutputKey, Seq[MapOutputValue])]): Iterator[(ReduceOutputKey, ReduceOutputValue)] = {    
      val mapOutputKeyClass: Class[MapOutputKey] = 
        implicitly[ClassManifest[MapOutputKey]].erasure.asInstanceOf[Class[MapOutputKey]]
      val mapOutputValueClass: Class[MapOutputValue] = 
        implicitly[ClassManifest[MapOutputValue]].erasure.asInstanceOf[Class[MapOutputValue]]
      val reducerClass: Class[Reducer[MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue]] = 
        jobContext.getReducerClass().asInstanceOf[Class[Reducer[MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue]]]

      val comparator: RawComparator[MapOutputKey] = jobConf.getOutputKeyComparator().asInstanceOf[RawComparator[MapOutputKey]]
      val reducer: Reducer[MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue] = createNewInstance(reducerClass)
      
      // Sorting the keys using the job's comparator, just like Hadoop does
      val useSort = jobConf.get("mapred.sort.disable", "false") != "true"
      val isWritableComparable = classOf[WritableComparable[MapOutputKey]].isAssignableFrom(mapOutputKeyClass)
      
      val inputIterator = if (useSort) {
        val seq = iterator.toSeq
        println("Sorting " + seq.size + " keys")
        val startTime = System.currentTimeMillis()
        val sortedSeq = seq.sortWith( (x: (MapOutputKey, Seq[MapOutputValue]), y: (MapOutputKey, Seq[MapOutputValue])) => {
          if (isWritableComparable) {
            x._1.asInstanceOf[WritableComparable[MapOutputKey]].compareTo(y._1) < 0
          } else {
            val xBytes = WritableUtils.toByteArray(x._1)
                val yBytes = WritableUtils.toByteArray(y._1)
                comparator.compare(xBytes, 0, xBytes.length, yBytes, 0, yBytes.length) < 0
          }
        })
        println("Sorted in " + (System.currentTimeMillis() - startTime) + " ms")
        sortedSeq.iterator        
      } else {
        iterator
      }
      
      // This dummy iterator is used in the creation of reducer context as ReduceContext checks the availability of key-values in the constructor
      // It is not used anywhere else in this custom Reducer.Context
      val dummyIterator = new RawKeyValueIterator {
        def getKey(): DataInputBuffer = null
        def getValue(): DataInputBuffer = null
        def getProgress(): Progress = null
        def next() = inputIterator.hasNext
        def close() { }
      }
      
      val reducerContext = new reducer.Context(jobConf, new TaskAttemptID(), dummyIterator, 
          null, null, null, null, new WrappedReporter(), null, mapOutputKeyClass, mapOutputValueClass) {
        // Option and None used instead of null because of this issue: 
        // http://stackoverflow.com/questions/7830731/parameter-type-in-structural-refinement-may-not-refer-to-an-abstract-type-defin
        var currentKey: Option[MapOutputKey] = None
        var currentValue: Option[MapOutputValue] = None
        var currentValueIterator: java.util.Iterator[MapOutputValue] = null
        val outputBuffer = new ArrayBuffer[(ReduceOutputKey, ReduceOutputValue)]()
        
        override def nextKey() = if (inputIterator.hasNext) {          
          val (key, values) = inputIterator.next()
          currentKey = Some(key)
          currentValueIterator = new java.util.Iterator[MapOutputValue]() {
            val iterator = values.iterator
            def hasNext() = iterator.hasNext
            def next() = iterator.next()              
            def remove() { }
          }
          true
        } else {
          currentKey = None
          currentValueIterator = null
          false
        }
        
        override def nextKeyValue() = {
          if (currentKey == null) {
            if (!nextKey()) {        
              false
            } else {
              if (currentValueIterator.hasNext()) {
                currentValue = Some(currentValueIterator.next())
                true
              } else {
                currentValue = None
                false
              }
            }        
          } else {          
            if (currentValueIterator.hasNext()) {
              currentValue = Some(currentValueIterator.next())
              true
            } else {
              currentValue = None
              false
            }
          }
        }
        
        override def getCurrentKey(): MapOutputKey = 
          if (currentKey.isDefined) currentKey.get else null.asInstanceOf[MapOutputKey]
        
        override def getCurrentValue(): MapOutputValue = 
          if (currentValue.isDefined) currentValue.get else null.asInstanceOf[MapOutputValue] 
        
        override def getValues(): java.lang.Iterable[MapOutputValue] = new java.lang.Iterable[MapOutputValue]() {
          def iterator(): java.util.Iterator[MapOutputValue] = currentValueIterator
        }
        
        override def write(key: ReduceOutputKey, value: ReduceOutputValue) {
          outputBuffer += ((WritableUtils.clone(key, jobConf), value))
        }
        
        def outputIterator() = outputBuffer.toIterator
      }
        
      reducer.run(reducerContext)
      reducerContext.outputIterator
    }
  }
  
  /**
   * This subclass is a wrapper around Hadoop Partitioner (new API). It implements a custom Reducer.Context
   * that is given to the mapper object for processing the data.
   */  
  class WrappedPartitioner(numReducers: Int) extends Partitioner() {
    
    @transient var hPartitioner: HadoopPartitioner[MapOutputKey, MapOutputValue] = null
    
    def hadoopPartitioner(): HadoopPartitioner[MapOutputKey, MapOutputValue] = {
      if (hPartitioner == null) {
        val partitionerClass = jobContext.getPartitionerClass().asInstanceOf[Class[HadoopPartitioner[MapOutputKey, MapOutputValue]]]   
        hPartitioner = createNewInstance(partitionerClass)
      }
      hPartitioner
    }
    
    override def numPartitions = numReducers
    
    override def getPartition(key: Any): Int = {      
      hadoopPartitioner.getPartition(key.asInstanceOf[MapOutputKey], null.asInstanceOf[MapOutputValue], numPartitions)
    }
  }
  
  val jobConfSerializable = new SerializableWritable[JobConf](job.getConfiguration().asInstanceOf[JobConf])
  @transient var jobContextTransient: JobContext = null
  
  def jobConf(): JobConf = jobConfSerializable.value 
  
  def jobContext(): JobContext = {
    if (jobContextTransient == null) {
      jobContextTransient = new JobContext(jobConf, new JobID()) 
    }
    jobContextTransient
  }
  
  def createNewInstance[T](cls: Class[T]): T = {
    ReflectionUtils.newInstance[T](cls, jobConf)
  }
  
  def run(sc: SparkContext) {   
    // Preparing the required objects for running the spark job
    val inputPaths: Iterable[String] = FileInputFormat.getInputPaths(jobContext).map(_.toString)
    val outputPath: String = FileOutputFormat.getOutputPath(jobContext).toString()
     
    // Classes for reading input data by the Mapper 
    val mapInputKeyClass: Class[MapInputKey] = 
      implicitly[ClassManifest[MapInputKey]].erasure.asInstanceOf[Class[MapInputKey]]
    val mapInputValueClass: Class[MapInputValue] = 
      implicitly[ClassManifest[MapInputValue]].erasure.asInstanceOf[Class[MapInputValue]]
    val inputFormatClass: Class[InputFormat[MapInputKey, MapInputValue]] =
      jobContext.getInputFormatClass().asInstanceOf[Class[InputFormat[MapInputKey, MapInputValue]]]
    println(mapInputKeyClass + ", " + mapInputValueClass)
    // Classes used for outputting data by the Reducer
    val reduceOutputKeyClass: Class[ReduceOutputKey] = 
      implicitly[ClassManifest[ReduceOutputKey]].erasure.asInstanceOf[Class[ReduceOutputKey]]   
    val reduceOutputValueClass: Class[ReduceOutputValue] = 
      implicitly[ClassManifest[ReduceOutputValue]].erasure.asInstanceOf[Class[ReduceOutputValue]]
    val outputFormatClass: Class[OutputFormat[_, _]] =
      jobContext.getOutputFormatClass().asInstanceOf[Class[OutputFormat[_, _]]]    
     
    // Spark's wrapper around the Hadoop mapper class to do the map phase
    val wrapperMapper = new WrappedMapper() 
    
    // Spark's Partitioner object to custom partition the map output RDD
    val numReducers = jobContext.getNumReduceTasks()
    println("# reducers = " + numReducers)
    val wrappedPartitioner = new WrappedPartitioner(numReducers)
    
    // Spark's wrapper around the Hadoop reduceer class to do the reduce phase
    val wrappedReducer = new WrappedReducer()
     
    // Running the Spark job
    println("Processing " + inputPaths.size + " input paths")
    val inputRDDs: Seq[RDD[(MapInputKey, MapInputValue)]] = inputPaths.map(path => {
      sc.newAPIHadoopFile[MapInputKey, MapInputValue, InputFormat[MapInputKey, MapInputValue]](path, inputFormatClass, mapInputKeyClass, mapInputValueClass, jobConf)
      //sc.hadoopFile(path, inputFormatClass, mapInputKeyClass, mapInputValueClass)
    }).toSeq
    val unifiedInputRDDs = new UnionRDD[(MapInputKey, MapInputValue)](sc, inputRDDs.toSeq) 
    val mappedRDD: RDD[(MapOutputKey, MapOutputValue)] = unifiedInputRDDs.mapPartitions(wrapperMapper.run)
    val groupedRDD: RDD[(MapOutputKey, Seq[MapOutputValue])] = mappedRDD.groupByKey(wrappedPartitioner)
    val reducedRDD: RDD[(ReduceOutputKey, ReduceOutputValue)] = groupedRDD.mapPartitions(wrappedReducer.run)
    reducedRDD.saveAsNewAPIHadoopFile(outputPath, reduceOutputKeyClass, reduceOutputValueClass, outputFormatClass) 
  }    
}
 

object SparkHadoopJob {
  
  def runTestJob() {
    val sc = new SparkContext("local[2]", "Test")
    sc.parallelize(1 to 100, 10).map(x => (x % 10, 1)).reduceByKey(_ + _).collect().foreach(println)
    // sc.stop()
  }
  
  def runWordCountJob(args: Array[String]) {
    if (args.length < 1) {
      println("Too few parameters")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputFile = inputFile + "_counts_spark"
    System.setProperty("spark.kryo.registrator", "testscala.WritableRegistrator")
      
    try {
      val wcJob = new WordCountJob(inputFile, outputFile)
      wcJob.prepareJob();
      
      val sc = new SparkContext("local[2]", "WordCount")
      val hadoopJob = wcJob.getJob()
      //hadoopJob.getConfiguration().set("mapred.sort.disable", "true")
      val sparkHadoopJob = new SparkHadoopJob[LongWritable, Text, Text, LongWritable, Text, NullWritable](hadoopJob)
      sparkHadoopJob.run(sc)
      sc.stop()
    } catch {
    case e: java.io.IOException => e.printStackTrace()
    case ie: InterruptedException => ie.printStackTrace();
    case ce: ClassNotFoundException => ce.printStackTrace();
    }
  }
  
  
  def main(args: Array[String]) {
    runTestJob()
    //runWordCountJob(args)
  }
}
      

  /*
  
  //val mapperInput = new SparkRecordReader[MapInputKey, MapInputValue](iterator)
  //val mapperOutput = new SparkRecordWriter[MapOutputKey, MapOutputValue]()
  //val mapperContext = new mapper.Context(job.getConfiguration(), null, mapperInput, mapperOutput, null, null, null)    
  //mapperClass = jobConf.t.getMapperClass().asInstanceOf[Class[Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue]]]  

  
  class SparkRecordReader[WriterKey, WriterValue](iterator: Iterator[(WriterKey, WriterValue)]) 
  extends RecordReader[WriterKey, WriterValue] {
    var currentKeyValue: (WriterKey, WriterValue) = null
    override def initialize(s: InputSplit, c: TaskAttemptContext) { }
    override def nextKeyValue() = if (iterator.hasNext) {
      currentKeyValue = iterator.next()
      true
    } else {
      false
    }      
    override def getCurrentKey(): WriterKey = if (currentKeyValue == null) 
      null.asInstanceOf[WriterKey] else currentKeyValue._1       
    override def getCurrentValue(): WriterValue = if (currentKeyValue == null) 
      null.asInstanceOf[WriterValue] else currentKeyValue._2     
    override def getProgress() = 0.0f
  }
  
  class SparkRecordWriter[WriterKey, WriterValue]() extends RecordWriter[WriterKey, WriterValue] {
    val buffer = new ArrayBuffer[(WriterKey, WriterValue)]()
    override def write(key: WriterKey, value: WriterValue) { buffer += ((key, value)) }
    override def close(c: TaskAttemptContext) { }
    def toIterator() = buffer.toIterator
  }
  */
  
  