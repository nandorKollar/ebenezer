//   Copyright 2014 Commonwealth Bank of Australia
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
package introspect

import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

class IntrospectionMaterializer(schema: MessageType) extends RecordMaterializer[Record] {
  var current: Option[Record] = None
  val converter = new IntrospectionRecordConverter(schema, record => current = Option(record))
  def getCurrentRecord = current.getOrElse(fail)
  def getRootConverter = converter
  def fail = sys.error("something really bad happended and it is hadoops fault so don't feel bad")
}
