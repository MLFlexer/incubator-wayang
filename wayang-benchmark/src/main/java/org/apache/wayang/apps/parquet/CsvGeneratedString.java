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

package org.apache.wayang.apps.parquet;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.java.operators.JavaParquetSource;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Arrays;
import org.apache.wayang.basic.data.Tuple2;

public class CsvGeneratedString {

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <input file>");
                System.exit(1);
            }

            WayangContext wayangContext = new WayangContext();
            wayangContext.register(Java.basicPlugin());

            /* Get a plan builder */
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("Generated")
                    .withUdfJarOf(Main.class);

            /* Start building the Apache WayangPlan */
            var plan = planBuilder
                    .readTextFile(args[0])
                    .withName("Read csv")
                    .map(row -> row.split(",")[0])
                    .withName("Get column 0")
                    .filter(i -> i.length() >= 5)
                    .withName("Filter negative")
                    .count();

            long startTime = System.nanoTime();
            Collection<Long> count = plan.collect();
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            System.out.printf("count is %d:\n", count.iterator().next());
            System.out.println("Execution time: " + duration + " nanoseconds");
            if (args.length > 1) {
              try (FileWriter fileWriter = new FileWriter(args[1], true)) {
                fileWriter.write(String.format("%s, %d\n", args[0], duration));
                System.out.println("Content appended successfully.");
              } catch (IOException e) {
                System.err.println("An error occurred while appending to the file: " + e.getMessage());
              }
            } 
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }
}

