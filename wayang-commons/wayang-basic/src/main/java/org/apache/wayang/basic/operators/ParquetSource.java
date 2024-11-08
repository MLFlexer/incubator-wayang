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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

/**
 * {@link UnarySource} that provides the tuples from a parquet file.
 */
public class ParquetSource extends UnarySource<Record> {

    private final String sourcePath;

    public String getSourcePath() {
        return sourcePath;
    }

    public ParquetSource(String sourcePath, DataSetType<Record> type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    public ParquetSource(String sourcePath) {
        this(sourcePath, createOutputDataSetType());
    }
    
    public ParquetSource(ParquetSource source) {
        this(source.sourcePath);
    }

    private static DataSetType<Record> createOutputDataSetType() {
        return DataSetType.createDefault(Record.class);
    }

}
