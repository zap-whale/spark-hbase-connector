/*
 * Copyright 2014 Databricks
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase.types

import org.apache.spark.sql.execution.datasources.hbase.{Field, HBaseType}

class Avro(f:Option[Field] = None) extends SHCDataType {

  private lazy val avroToCatalyst: Option[Any => Any] = {
    f.get.schema.map(SchemaConverters.createConverterToSQL)
  }

  private lazy val catalystToAvro: (Any) => Any ={
    SchemaConverters.createConverterToAvro(f.get.dt, f.get.colName, "recordNamespace")
  }

  def fromBytes(src: HBaseType): Any = {
    if (f.isDefined) {
      val m = AvroSerde.deserialize(src, f.get.exeSchema.get)
      val n = avroToCatalyst.map(_ (m))
      n.get
    } else {
      throw new UnsupportedOperationException(
        "Avro coder: without field metadata, 'fromBytes' conversion can not be supported")
    }
  }

  def toBytes(input: Any): Array[Byte] = {
    // Here we assume the top level type is structType
    if (f.isDefined) {
      val record = catalystToAvro(input)
      AvroSerde.serialize(record, f.get.schema.get)
    } else {
      throw new UnsupportedOperationException(
        "Avro coder: Without field metadata, 'toBytes' conversion can not be supported")
    }
  }
}
