/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.wayang.multicontext.apps.wordcount

import org.apache.wayang.api.implicits.DataQuantaAsyncResult
import org.apache.wayang.api.{BlossomContext, DataQuanta, MultiContextPlanBuilder, PlanBuilder}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.multicontext.apps.loadConfig
import org.apache.wayang.spark.Spark
import org.apache.wayang.api.implicits.DataQuantaImplicits._
import org.apache.wayang.api.implicits.PlanBuilderImplicits._

import scala.concurrent.Future

class Test {}

object Test {

  def main(args: Array[String]): Unit = {
    println("Counting words in multi context wayang!")
    println("Scala version:")
    println(scala.util.Properties.versionString)

    val (configuration1, configuration2) = loadConfig(args)

    val context1 = new BlossomContext(configuration1)
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out11")
    val context2 = new BlossomContext(configuration2)
      .withPlugin(Spark.basicPlugin())
      .withTextFileSink("file:///tmp/out12")

    val multiContextPlanBuilder = new MultiContextPlanBuilder(List(context1, context2))
      .withUdfJarsOf(classOf[WordCount])

    // Generate some test data
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count
    multiContextPlanBuilder
      .loadCollection(inputValues)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("\\W+", "").toLowerCase)
      .map((_, 1))
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2))
      .execute()

    val planBuilder1 = new PlanBuilder(context1).withUdfJarsOf(classOf[Test])
    val planBuilder2 = new PlanBuilder(context2).withUdfJarsOf(classOf[Test])
    val planBuilder3 = new PlanBuilder(new WayangContext(new Configuration())).withUdfJarsOf(classOf[Test])
    val planBuilder4 = new PlanBuilder(new WayangContext(new Configuration())).withUdfJarsOf(classOf[Test])


    val result1 = planBuilder1
      .loadCollection(List(1, 2, 3, 4, 5))
      .map(_ * 1)
      .runAsync(tempFileOut = "file:///tmp/out1.temp")

    val result2 = planBuilder2
      .loadCollection(List(6, 7, 8, 9, 10))
      .filter(_ <= 8)
      .runAsync(tempFileOut = "file:///tmp/out2.temp")

    val result3 = planBuilder3
      .combineFromAsync(result1, result2, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.union(dq2))
      .map(_ * 3)
      .runAsync(tempFileOut = "file:///tmp/out3.temp")

    val result4 = planBuilder4
      .loadCollection(List(1, 2, 3, 4, 5))
      .filter(_ >= 2)
      .runAsync(tempFileOut = "file:///tmp/out4.temp")

    val result5: Unit = planBuilder1
      .combineFromAsync(result3, result4, (dq1: DataQuanta[Int], dq2: DataQuanta[Int]) => dq1.intersect(dq2))
      .map(_ * 5)
      .writeTextFile("file:///tmp/out5.final", s => s.toString)

  }

}

