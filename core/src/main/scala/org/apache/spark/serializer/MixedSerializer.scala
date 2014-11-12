package org.apache.spark.serializer

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.SparkConf

import scala.reflect._

/**
 * A Spark serializer that chooses a serialization scheme based on objects and
 * a prioritized configuration.
 *
 * @param conf
 */
class MixedSerializer(conf: SparkConf) extends Serializer {

  override def newInstance(): SerializerInstance = {
    new MixedSerializerInstance(conf, this)
  }

}

private[spark] class MixedSerializationStream(s: OutputStream, serializer: MixedSerializerInstance)
    extends SerializationStream {

  var initialized: Boolean = false
  var wrappedStream: SerializationStream = null

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    if (!initialized) { initialize(t) }
    wrappedStream.writeObject(t)
    this
  }

  override def flush() {
    if (initialized) {
      wrappedStream.flush()
    }
  }

  override def close() {
    if (initialized) {
      wrappedStream.close()
    }
  }

  private def initialize[T: ClassTag](t: T) {
    this.initialized = true
    this.wrappedStream = serializer.forStream[T].serializeStream(s)
  }

}

private[spark] class MixedDeserializationStream(s: InputStream, serializer: MixedSerializerInstance)
  extends DeserializationStream {

  var initialized: Boolean = false
  var wrappedStream: DeserializationStream = null

  override def readObject[T: ClassTag](): T = {
    if (!initialized) { initialize[T] }
    wrappedStream.readObject()
  }

  override def close() {
    if (initialized) {
      wrappedStream.close()
    }
  }

  private def initialize[T: ClassTag] {
    this.initialized = true
    this.wrappedStream = serializer.forStream[T].deserializeStream(s)
  }

}

private[spark] class MixedSerializerInstance(conf: SparkConf, serializer: MixedSerializer)
  extends SerializerInstance {

  val avro = new AvroSerializer(conf).newInstance().asInstanceOf[AvroSerializerInstance]
  val kryo = new KryoSerializer(conf).newInstance().asInstanceOf[KryoSerializerInstance]

  val kryoInstance = kryo.getKryo

  // register additional clases
  kryoInstance.register(classOf[AvroSchema], new AvroSchemaSerializer())
  kryoInstance.register(classOf[GenericData.Record],
    new GenericAvroSerializer(classOf[GenericData.Record], avro))

  conf.get("spark.avro.classes", "").split(',').filter(!_.isEmpty)
    .foreach { className =>
      val datumClass: Class[_] = Class.forName(className)
      if (classOf[SpecificRecord].isAssignableFrom(datumClass)) {
        val specificClass = datumClass.asInstanceOf[Class[_ <: SpecificRecord]]
        kryoInstance.register(datumClass, new SpecificAvroSerializer(specificClass, avro))
      } else if (classOf[GenericRecord].isAssignableFrom(datumClass)) {
        val genericClass = datumClass.asInstanceOf[Class[_ <: GenericRecord]]
        kryoInstance.register(datumClass, new GenericAvroSerializer(genericClass, avro))
      } else {
//        kryoInstance.register(datumClass, new ReflectAvroSerializer[_](datumClass, avro))
      }
    }

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    forObject[T].serialize(t)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    forObject[T].deserialize[T](bytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    forObject[T].deserialize(bytes, loader)
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new MixedSerializationStream(s, this)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new MixedDeserializationStream(s, this)
  }

  def forObject[T: ClassTag]: SerializerInstance = {
    if (isKryo[T]) {
      kryo // prefer kryo if the type is registered
    } else if (avro.isAvro[T]) {
      avro
    } else {
      kryo // default to kryo, which may fail if registration is required
    }
  }

  def forStream[T: ClassTag]: SerializerInstance = {
    if (avro.isAvro[T]) {
      avro
    } else {
      kryo
    }
  }

  private def isKryo[T: ClassTag]: Boolean = {
    null != kryoInstance.getClassResolver.getRegistration(classTag[T].runtimeClass)
  }

}

private class AvroSchemaSerializer
  extends com.esotericsoftware.kryo.Serializer[AvroSchema] {

  override def write(kryo: Kryo, output: KryoOutput, schema: AvroSchema): Unit = {
    kryo.writeObject(output, schema.toString(false))
  }

  override def read(kryo: Kryo, input: KryoInput, schemaClass: Class[AvroSchema]): AvroSchema = {
    new AvroSchema.Parser().parse(kryo.readObject(input, classOf[String]))
  }

}

private class ReflectAvroSerializer[T: ClassTag](datumClass: Class[_], avro: AvroSerializerInstance)
  extends com.esotericsoftware.kryo.Serializer[T] {

  val schema: AvroSchema = avro.schemaOf(datumClass)

  override def write(kryo: Kryo, output: KryoOutput, datum: T): Unit = {
    kryo.writeObject(output, avro.serializeWithSchema(datum, schema).array())
  }

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[T]): T = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    avro.deserializeWithSchema(null, ByteBuffer.wrap(bytes), schema).asInstanceOf[T]
  }

}

private class SpecificAvroSerializer[T <: SpecificRecord: ClassTag](datumClass: Class[T], avro: AvroSerializerInstance)
    extends com.esotericsoftware.kryo.Serializer[T] {

  val schema: AvroSchema = specificSchema(datumClass)

  override def write(kryo: Kryo, output: KryoOutput, datum: T): Unit = {
    kryo.writeObject(output, avro.serializeWithSchema(datum, schema).array())
  }

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[T]): T = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    avro.deserializeWithSchema(null, ByteBuffer.wrap(bytes), schema).asInstanceOf[T]
  }

  private def specificSchema(datumClass: Class[_ <: SpecificRecord]): AvroSchema = {
    datumClass.getField("SCHEMA$").get(null).asInstanceOf[AvroSchema]
  }

}

private class GenericAvroSerializer[T <: GenericRecord: ClassTag](datumClass: Class[T], avro: AvroSerializerInstance)
    extends com.esotericsoftware.kryo.Serializer[T] {

  override def write(kryo: Kryo, output: KryoOutput, datum: T): Unit = {
    kryo.writeObject(output, avro.serialize(datum).array())
  }

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[T]): T = {
    val bytes = kryo.readObject(input, classOf[Array[Byte]])
    avro.deserialize(ByteBuffer.wrap(bytes))
  }

}
