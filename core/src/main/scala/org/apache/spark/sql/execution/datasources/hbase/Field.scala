/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._

import org.apache.avro.Schema
import org.apache.spark.sql.execution.datasources.hbase.types.SchemaConverters

object Field extends Logging {
  def apply(colName: String, cf: String, col: String, dt: DataType): Field = {
    new Field(colName, cf, col, dt, -1, "", None, None)
  }

  def apply(colName: String, cf: String, col: String, fCoder: String, sType: Option[String] = None, avroSchema: Option[String] = None, len: Int = -1): Field = {
    val schema: Option[Schema] = avroSchema.map { x =>
      logDebug(s"avro: $x")
      val p = new Schema.Parser
      p.parse(x)
    }
    val dt =
      if (avroSchema.isDefined)
        schema.map(SchemaConverters.toSqlType(_).dataType).get
      else
        sType.map(CatalystSqlParser.parseDataType).get
    new Field(colName, cf, col, dt, len, fCoder, sType, avroSchema)
  }
}

// The definition of each column cell, which may be composite type
case class Field(
    val colName: String,
    val cf: String,
    val col: String,
    val dt: DataType,
    val len: Int,
    val fCoder: String,
    val sType: Option[String],
    val avroSchema: Option[String]) extends Logging {

  val isRowKey = cf == SparkHBaseConf.rowKey
  var start: Int = _

  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema = schema

  // converter from avro to catalyst structure
  lazy val avroToCatalyst: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL)
  }

  // converter from catalyst to avro
  lazy val catalystToAvro: (Any) => Any ={
    SchemaConverters.createConverterToAvro(dt, colName, "recordNamespace")
  }

  val length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType => Bytes.SIZEOF_BOOLEAN
        case ByteType => 1
        case DoubleType => Bytes.SIZEOF_DOUBLE
        case FloatType => Bytes.SIZEOF_FLOAT
        case IntegerType => Bytes.SIZEOF_INT
        case LongType => Bytes.SIZEOF_LONG
        case ShortType => Bytes.SIZEOF_SHORT
        case _ => -1
      }
    } else {
      len
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      colName == that.colName && cf == that.cf && col == that.col
    case _ => false
  }
}
