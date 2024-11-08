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

package org.apache.wayang.basic.operators;


import org.apache.commons.lang3.Validate;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * This source reads a text file and outputs the lines as data units.
 */
public class ParquetSource extends UnarySource<String> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final String inputUrl;

    private final String encoding;

    public ParquetSource(String inputUrl) {
        this(inputUrl, "UTF-8");
    }

    public ParquetSource(String inputUrl, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.inputUrl = inputUrl;
        this.encoding = encoding;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ParquetSource(ParquetSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.encoding = that.getEncoding();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    /* // TODO: remove
    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        return Optional.empty();
    }
    */

    public String getEncoding() {
        return this.encoding;
    }

    // TODO: add CardinalityEstimator, see https://github.com/apache/incubator-wayang/blob/main/wayang-commons/wayang-basic/src/main/java/org/apache/wayang/basic/operators/TextFileSource.java#L93
}
