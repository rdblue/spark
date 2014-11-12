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
 */

package org.apache.spark.serializer

import java.io.{ByteArrayOutputStream, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericRecord, IndexedRecord, GenericData}
import org.apache.avro.io._
import org.apache.avro.reflect.ReflectData
import org.apache.avro.{Schema => AvroSchema, SchemaBuilder => AvroSchemaBuilder}
import org.apache.spark._

import scala.reflect._

/**
 * A Spark serializer that uses [[https://avro.apache.org/ Avro]] for
 * serialization.
 *
 * Note that this serializer is not guaranteed to be wire-compatible across
 * different versions of Spark. It is intended to be used to
 * serialize/de-serialize data within a single Spark application.
 */
class AvroSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  override def newInstance(): SerializerInstance = {
    new AvroSerializerInstance(conf, this)
  }

}

private[spark]
class AvroTypedSerializationStream[T: ClassTag](outStream: OutputStream, avro: AvroSerializerInstance)
    extends SerializationStream {

  private var initialized = false
  private var datumClass: Class[_ <: T] = null
  private var schema: AvroSchema = null
  private var writer: DatumWriter[T] = null
  private var fileWriter: DataFileWriter[T] = null

  override def writeObject[S: ClassTag](s: S): SerializationStream = {
    s match {
      case t if datumClass.isAssignableFrom(t.getClass) => writeObject(t)
      case _ => throw new RuntimeException("Cannot write as " + s.getClass)
    }
    this
  }

  def writeObject(t: T) {
    if (!initialized) { initialize(t) }
    fileWriter.append(t)
  }

  override def flush() {
    if (initialized) {
      fileWriter.flush()
      outStream.flush()
    }
  }

  override def close() {
    if (initialized) {
      fileWriter.close() // closes underlying stream
    }
  }

  private def initialize(t: T) = {
    this.initialized = true
    this.datumClass = t.getClass
    this.schema = avro.schemaOf(t)
    this.writer = ReflectData.get.createDatumWriter(schema).asInstanceOf[DatumWriter[T]]
    this.fileWriter = new DataFileWriter(writer)
    fileWriter.create(schema, outStream)
  }

}

private[spark]
class AvroTypedDeserializationStream[T: ClassTag](inStream: InputStream, avro: AvroSerializerInstance)
    extends DeserializationStream {

  private val reader = new DataFileStream[T](inStream,
    ReflectData.get.createDatumReader(null).asInstanceOf[DatumReader[T]])
  private var reused: T = null.asInstanceOf[T]

  override def readObject[S: ClassTag](): S = {
    val t: T = readTypedObject()
    t match {
      case s : S => t.asInstanceOf[S]
      case _ => throw new RuntimeException("Cannot read as " + classTag[S].runtimeClass)
    }
  }

  def readTypedObject(): T = {
    if (!reader.hasNext) {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      throw new EOFException
    }

    this.reused = reader.next(reused)

    reused
  }

  def close() {
    reader.close() // closes underlying stream
  }
}

private[spark] class AvroSerializerInstance(conf: SparkConf, avro: AvroSerializer)
    extends SerializerInstance {

  private val writerCache = collection.mutable.Map[AvroSchema, DatumWriter[_]]()
  private val readerCache = collection.mutable.Map[AvroSchema, DatumReader[_]]()
  private val outBuffer = new ByteArrayOutputStream()
  private val reusedEncoder = EncoderFactory.get.binaryEncoder(outBuffer, null)
  private val reusedDecoder = DecoderFactory.get.binaryDecoder(new Array[Byte](0), null)

  //private lazy val parser = new AvroSchema.Parser()

  // use a known record schema to avoid writing file overhead
  private val serializedObjectSchema : AvroSchema = AvroSchemaBuilder
    .record("SerializedObject").fields()
    .requiredString("schema")
    .requiredBytes("bytes")
    .endRecord()

  private val objectWrapper = new GenericData.Record(serializedObjectSchema)

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val objectSchema = schemaOf(t)
    val objectBytes: ByteBuffer = serializeWithSchema(t, objectSchema)

    objectWrapper.put(0, objectSchema.toString(false))
    objectWrapper.put(1, objectBytes)

    serializeWithSchema(objectWrapper, serializedObjectSchema)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    deserialize(bytes, Thread.currentThread.getContextClassLoader)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val wrappedObject = deserializeWithSchema(objectWrapper, bytes, serializedObjectSchema, loader)
    val objectSchema = new AvroSchema.Parser().parse(wrappedObject.get(0).asInstanceOf[String])
    val objectBytes = wrappedObject.get(1).asInstanceOf[ByteBuffer]

    deserializeWithSchema(null, objectBytes, objectSchema, loader).asInstanceOf[T]
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new AvroTypedSerializationStream(s, this)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new AvroTypedDeserializationStream(s, this)
  }

  def serializeWithSchema[T: ClassTag](t: T, schema: AvroSchema): ByteBuffer = {
    outBuffer.reset()
    val encoder = EncoderFactory.get.binaryEncoder(outBuffer, reusedEncoder)
    val writer = writerCache.getOrElseUpdate(
      schema, ReflectData.get.createDatumWriter(schema))
      .asInstanceOf[DatumWriter[T]]
    writer.write(t, encoder)
    encoder.flush()
    ByteBuffer.wrap(outBuffer.toByteArray) // returns a newly allocated array
  }

  def deserializeWithSchema[T](datum: T, bytes: ByteBuffer, schema: AvroSchema): T = {
    deserializeWithSchema(datum, bytes, schema, Thread.currentThread().getContextClassLoader)
  }

  def deserializeWithSchema[T](datum: T,
                                       bytes: ByteBuffer,
                                       schema: AvroSchema,
                                       loader: ClassLoader): T = {
    // TODO: use the class loader
    val decoder = DecoderFactory.get.binaryDecoder(bytes.array(), reusedDecoder)
    val reader = readerCache.getOrElseUpdate(
      schema, ReflectData.get.createDatumReader(schema))
      .asInstanceOf[DatumReader[T]]
    reader.read(datum, decoder)
  }

  def schemaOf(datum: Any): AvroSchema = {
    if (datum == null) {
      return AvroSchema.create(AvroSchema.Type.NULL)
    }

    datum match {
      case record: GenericRecord =>
        record.getSchema
      case _ =>
        schemaOf(datum.getClass)
    }
  }
  
  def schemaOf(datumClass: Class[_]): AvroSchema = {
    val key = schemaKey(datumClass)

    // check for a schema for this class in the SparkConf
    if (conf.contains(key)) {
      new AvroSchema.Parser().parse(conf.get(key))
    } else {
      ReflectData.AllowNull.get.getSchema(datumClass)
    }
  }

  private def schemaKey(datumClass: Class[_]): String = {
    "avro.schema." + datumClass.getCanonicalName
  }

  def isAvro[T: ClassTag]: Boolean = {
    val runtimeClass = classTag[T].runtimeClass
    classOf[IndexedRecord].isAssignableFrom(runtimeClass)
    conf.contains(schemaKey(runtimeClass))
  }

}

object AvroUtil {

}
