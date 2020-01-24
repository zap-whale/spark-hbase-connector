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

import java.io.{IOException, ByteArrayInputStream}

import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DatumReader, DecoderFactory, EncoderFactory}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}

import org.apache.commons.io.output.ByteArrayOutputStream

object AvroSerde {
  // We only handle top level is record or primary type now
  def serialize(input: Any, schema: Schema): Array[Byte]= {
    val bao = new ByteArrayOutputStream()
    try {
      val writer = new GenericDatumWriter[Any](schema)
      val encoder: BinaryEncoder = EncoderFactory.get().directBinaryEncoder(bao, null)
      writer.write(input, encoder)
      bao.toByteArray()
    } catch {
      case e: IOException => throw new Exception(s"Exception while creating byte array for Avro schema ${schema}")
    } finally {
      if(bao != null) {
        bao.close()
      }
    }
  }

  def deserialize(input: Array[Byte], schema: Schema): Any = {
    val reader2: DatumReader[Any] = new GenericDatumReader[Any](schema)
    val bai2 = new ByteArrayInputStream(input)
    val decoder2: BinaryDecoder = DecoderFactory.get().directBinaryDecoder(bai2, null)
    val res: Any = reader2.read(null, decoder2)
    res
  }
}
