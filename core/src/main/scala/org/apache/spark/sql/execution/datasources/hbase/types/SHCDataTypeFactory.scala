/*
 * (C) 2017 Hortonworks, Inc. All rights reserved. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. This file is licensed to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase.types

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Currently, SHC supports three data types which can be used as serdes: Avro, Phoenix, PrimitiveType.
 * Adding New SHC data type needs to implement the trait 'SHCDataType'.
 */
object SHCDataTypeFactory {

  def create(f: Field): SHCDataType = {
    if (f == null) {
      throw new NullPointerException(
        "SHCDataTypeFactory: the 'f' parameter used to create SHCDataType " +
          "can not be null.")
    }

    val className = getClassName(f.fCoder)
    Class.forName(className)
        .getConstructor(classOf[Option[Field]])
        .newInstance(Some(f))
        .asInstanceOf[SHCDataType]
  }

  // Currently, the function below is only used for creating the table coder.
  // One catalog/HBase table can only use one table coder, so the function is
  // only called once in 'HBaseTableCatalog' class.
  def create(coder: String): SHCDataType = {
    if (coder == null || coder.isEmpty) {
      throw new NullPointerException(
        "SHCDataTypeFactory: the 'coder' parameter used to create SHCDataType " +
          "can not be null or empty.")
    }

    val className = getClassName(coder)
    Class.forName(getClassName(coder))
        .getConstructor(classOf[DataType])
        .newInstance(StringType)
        .asInstanceOf[SHCDataType]
  }

  def create(serializedType: String, coder: String = SparkHBaseConf.PrimitiveType): SHCDataType = {
    Class.forName(getClassName(coder))
        .getConstructor(classOf[DataType])
        .newInstance(StringType)
        .asInstanceOf[SHCDataType]
  }

  private def getClassName(coderName: String): String = coderName match {
    case SparkHBaseConf.PrimitiveType =>
      "org.apache.spark.sql.execution.datasources.hbase.types.PrimitiveType"		
    case SparkHBaseConf.Phoenix =>
      "org.apache.spark.sql.execution.datasources.hbase.types.Phoenix"
    case SparkHBaseConf.Avro =>
      "org.apache.spark.sql.execution.datasources.hbase.types.Avro"
    case name: String =>
      name
  }
}
