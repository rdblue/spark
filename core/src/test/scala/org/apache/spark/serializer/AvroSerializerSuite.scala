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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.reflect.ClassTag

class AvroSerializerSuite extends FunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.MixedSerializer")

  private val testSchema: AvroSchema = new AvroSchema.Parser().parse("{\n" +
    "  \"type\" : \"record\",\n" +
    "  \"name\" : \"Test\",\n" +
    "  \"fields\" : [ {\n" +
    "    \"name\" : \"word\",\n" +
    "    \"type\" : \"string\"\n" +
    "  }, {\n" +
    "    \"name\" : \"size\",\n" +
    "    \"type\" : [ \"null\", \"int\" ],\n" +
    "    \"default\" : null\n" +
    "  } ]\n" +
    "}")

  test("basic types") {
    val ser = new MixedSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      t match {
        case s : AvroSchema =>
          assert(ser.deserialize[T](ser.serialize(t)).toString === t.toString)
        case _ =>
          assert(ser.deserialize[T](ser.serialize(t)) === t)
      }
    }
    check(1)
    check(1L)
    check(1.0f)
    check(1.0)
    check(1.toByte)
    check(1.toShort)
    check("")
    check("hello")
    check(Integer.MAX_VALUE)
    check(Integer.MIN_VALUE)
    check(java.lang.Long.MAX_VALUE)
    check(java.lang.Long.MIN_VALUE)
    check[String](null)
    check(Array(1, 2, 3))
    check(Array(1L, 2L, 3L))
    check(Array(1.0, 2.0, 3.0))
    check(Array(1.0f, 2.9f, 3.9f))
    check(Array("aaa", "bbb", "ccc"))
    check(Array("aaa", "bbb", null))
    check(Array(true, false, true))
    check(Array('a', 'b', 'c'))
    check(Array[Int]())
    check(Array(Array("1", "2"), Array("1", "2", "3", "4")))
    check(makeRecord("bicycle"))
    check(new User(34L, "green", "green@example.com"))
    check(testSchema)
  }

  test("Avro generics with collect") {
    val control = Array( "apple", "chainsaw", "happy", "idempotent" )

    val records = control.map(makeRecord)

    val result = sc.parallelize(records, 2).map { record =>
        record.put("size", record.get("word").asInstanceOf[String].length)
        record
      }.collect().map(_.get("size"))

    assert(control.map(_.length) === result.toSeq)
  }

  test("Avro specifics with collect") {
    conf.set("spark.avro.classes", "org.apache.spark.serializer.User")
    val control = Array( "maroon", "black", "white", "aqua" )

    val result = sc.parallelize(control, 2).map { name =>
      new User(name.hashCode.toLong, name, name + "@example.com")
    }.collect().map(_.getEmail.length)

    assert(control.map(_.length + 12) === result.toSeq)
  }

  def makeRecord(word: String): GenericRecord = {
    val record = new GenericData.Record(testSchema)
    record.put("word", word)
    record
  }

  test("pairs") {
    conf.set("spark.avro.classes", "org.apache.spark.serializer.User")
    val ser = new MixedSerializer(conf).newInstance()
    def check[T: ClassTag](t: T) {
      assert(ser.deserialize[T](ser.serialize(t)) === t)
    }
    check((1, 1))
    check((1, 1L))
    check((1L, 1))
    check((1L,  1L))
    check((1.0, 1))
    check((1, 1.0))
    check((1.0, 1.0))
    check((1.0, 1L))
    check((1L, 1.0))
    check((1.0, 1L))
    check(("x", 1))
    check(("x", 1.0))
    check(("x", 1L))
    check((1, "x"))
    check((1.0, "x"))
    check((1L, "x"))
    check(("x", "x"))
    check((makeRecord("fuzzy"), 5))
    check(("drip", makeRecord("controlling")))
    check((new User(1L, "red", "red@example.com"), 34L))
    check(("x", new User(2L, "orange", "orange@example.com")))
  }

  test("Avro generic record stream") {
    val ser = new MixedSerializer(conf).newInstance()
    val stream = new ByteArrayOutputStream()
    def check(list: List[GenericRecord]): Unit = {
      val out = ser.serializeStream(stream)
      list.foreach { record =>
        out.writeObject(record)
      }
      out.flush()
      val in = ser.deserializeStream(new ByteArrayInputStream(stream.toByteArray))
      val iter = in.asIterator
      list.foreach { record =>
        assert(iter.next === record)
      }
      assert(!iter.hasNext)
    }
    check(List(makeRecord("a"), makeRecord("b"), makeRecord("c")))
  }

  test("Avro specific record stream") {
    val ser = new MixedSerializer(conf).newInstance()
    val stream = new ByteArrayOutputStream()
    def check(list: List[GenericRecord]): Unit = {
      val out = ser.serializeStream(stream)
      list.foreach { record =>
        out.writeObject(record)
      }
      out.flush()
      val in = ser.deserializeStream(new ByteArrayInputStream(stream.toByteArray))
      val iter = in.asIterator
      list.foreach { record =>
        assert(iter.next === record)
      }
      assert(!iter.hasNext)
    }

    val records = List( "maroon", "black", "white", "aqua" ).map { name =>
      new User(name.hashCode.toLong, name, name + "@example.com")
    }

    check(records)
  }

}
