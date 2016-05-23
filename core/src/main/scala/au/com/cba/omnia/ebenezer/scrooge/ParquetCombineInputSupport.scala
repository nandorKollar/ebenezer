package au.com.cba.omnia.ebenezer.scrooge

import java.io.{DataInput, DataOutput}
import java.util

import collection.JavaConversions._

import org.apache.hadoop.mapred._

import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetInputSplit, ParquetRecordReader}
import parquet.hadoop.mapred.{Container, DeprecatedParquetInputFormat}

/**
  * Eternal workaround for parquet 1.5 and cascading 2.6.1
  * TODO: all the classes in this file need to be rewrote if we depend on a new parquet/cascading version
  */
class DeprecatedParquetCombineInputFormat[T] extends DeprecatedParquetInputFormat[T] {
  override def getSplits(job: JobConf, numSplits: Int) = {
    val footers = getFooters(job)
    val mapreduceFileSplits = realInputFormat.getSplits(job, footers)

    if (mapreduceFileSplits == null)
      null
    else
      mapreduceFileSplits.map(new ParquetFileSplitWrapper(_)).toArray
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter) =
    new CombineRecordReader[T](realInputFormat, split, job, reporter)
}

/**
  * TODO: This class needs to be rewrote for parquet 1.6
  * TODO: This class can be removed for parquet 1.7+
  * Reference: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/mapred/DeprecatedParquetInputFormat.java#L40
  *
  * MORE: Hadoop has 2 sets of API, mapred(aka mrv1) API and mapreduce(aka mrv2) API. Cascading version we are using is
  * based on mrv1, and the parquet version in CDH release we are using is based on mrv2. In the latest parquet release,
  * or the beta release of cascading, they support both v1 and v2 API. All the classes here are trying to implement a
  * wrapper to convert ParquetInputSplit(extends mrv2.FileSplit) into mrv1.FileSplit in both plan and execution stage.
  */
class CombineRecordReader[V](
  newInputFormat: ParquetInputFormat[V],
  oldSplit: InputSplit,
  oldJobConf: JobConf,
  reporter: Reporter
) extends RecordReader[Void, Container[V]] {
  val realReader    : ParquetRecordReader[V] =
    new ParquetRecordReader(newInputFormat.getReadSupport(oldJobConf), ParquetInputFormat.getFilter(oldJobConf))
  val splitLen      : Long                   = oldSplit.getLength
  var valueContainer: Container[V]           = null
  var firstRecord   : Boolean                = false
  var eof           : Boolean                = false

  oldSplit match {
    case wrapper: ParquetFileSplitWrapper =>
      // plan stage
      realReader.initialize(oldSplit.asInstanceOf[ParquetFileSplitWrapper].realSplit, oldJobConf, reporter)
    case split: FileSplit =>
      // runtime
      val oldFileSplit = oldSplit.asInstanceOf[FileSplit]
      val finalPath = oldFileSplit.getPath
      val parquetMetadata = ParquetFileReader.readFooter(oldJobConf, finalPath)
      val blocks = parquetMetadata.getBlocks
      val readerSupportClass = ParquetInputFormat.getReadSupportClass(oldJobConf).newInstance().asInstanceOf[ReadSupport[V]]
      val readContext = readerSupportClass.init(
        new InitContext(
          oldJobConf,
          parquetMetadata.getFileMetaData.getKeyValueMetaData.mapValues {
            value =>
              val set = new util.HashSet[String]()
              set.add(value)
              set
          },
          parquetMetadata.getFileMetaData.getSchema
        ))

      val newSplit = new ParquetInputSplit(
        oldFileSplit.getPath,
        oldFileSplit.getStart,
        oldFileSplit.getLength,
        oldFileSplit.getLocations,
        blocks,
        readContext.getRequestedSchema.toString,
        parquetMetadata.getFileMetaData.getSchema.toString.intern,
        parquetMetadata.getFileMetaData.getKeyValueMetaData,
        readContext.getReadSupportMetadata
      )
      realReader.initialize(newSplit, oldJobConf, reporter)
    case _ =>
      throw new IllegalArgumentException("Invalid Split: " + oldSplit)
  }

  if (realReader.nextKeyValue()) {
    firstRecord = true
    valueContainer = new Container()
    valueContainer.set(realReader.getCurrentValue)
  } else {
    eof = true
  }

  def close() = realReader.close()

  def createKey() = null

  def createValue() = valueContainer

  def getPos = (splitLen.toFloat * getProgress).toLong

  def getProgress = realReader.getProgress

  def next(key: Void, value: Container[V]): Boolean = {
    if (eof)
      false
    else if (firstRecord) {
      firstRecord = false
      true
    } else {
      if (realReader.nextKeyValue()) {
        if (value != null)
          value.set(realReader.getCurrentValue)
        true
      }
      else {
        eof = true
        false
      }
    }
  }
}

/**
  * TODO: This class can be removed for parquet 1.7+
  */
class ParquetFileSplitWrapper(split: ParquetInputSplit)

  extends FileSplit(null, 0, 0, new Array[String](0)) {

  var realSplit = split

  def this() = this(null) // hadoop need this default constructor

  override def write(out: DataOutput) = realSplit.write(out)

  override def readFields(in: DataInput) = {
    realSplit = new ParquetInputSplit()
    realSplit.readFields(in)
  }

  override def getLength = realSplit.getLength

  override def getLocations = realSplit.getLocations

  override def getStart = realSplit.getStart

  override def getPath = realSplit.getPath

  override def toString = realSplit.toString
}
