//   Copyright 2015 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer
package scrooge

import cascading.tap.Tap

import com.twitter.scalding._
import com.twitter.scalding.typed.PartitionUtil

import com.twitter.scrooge.ThriftStruct

case class PartitionParquetScroogeSource[A, T <: ThriftStruct](template: String, path: String)(
  implicit m : Manifest[T], valueConverter: TupleConverter[T], partitionConverter: TupleConverter[A]
) extends Source 
  with Mappable[T]
  with java.io.Serializable {

  val partition = {
    val templateFields = PartitionUtil.toFields(valueConverter.arity, valueConverter.arity + partitionConverter.arity)
    new TemplatePartition(templateFields, template)
  }

  val hdfsScheme = ParquetScroogeSchemeSupport.parquetHdfsScheme[T]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = mode match {
    case hdfsMode @ Hdfs(_, jobConf) => readOrWrite match {
      case Read  => CastHfsTap(new PartitionParquetScroogeReadTap(path, partition, hdfsScheme))
      case Write =>
        sys.error(s"HDFS write mode is currently not supported for ${toString}. Use PartitionHiveParquetScroogeSink instead.")
    }
    case Local(_) => sys.error(s"Local mode is currently not supported for ${toString}")
    case x        => sys.error(s"$x mode is currently not supported for ${toString}")
  }
  
  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](valueConverter)

  override def toString: String =
    s"PartitionParquetScroogeSource[${m.runtimeClass}]($path, $template)"
}
