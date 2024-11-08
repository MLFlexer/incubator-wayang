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

package org.apache.wayang.java.operators;

import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

/**
 * Java implementation for the {@link ParquetSource}.
 */
public class JavaParquetSource<T> extends ParquetSource implements JavaExecutionOperator {
    private final Schema schema;

    public JavaParquetSource(String sourcePath, Schema schema) {
        super(sourcePath);
        this.schema = schema;
    }

    public JavaParquetSource(String sourcePath) {
        super(sourcePath);
        this.schema = null;
    }

    public JavaParquetSource(ParquetSource that) {
        super(that);
        this.schema = null;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OperatorContext operatorContext) {
        assert outputs.length == this.getNumOutputs();

        final String path = this.getSourcePath();
        Stream<Record> stream = this.createStream(path);

        ((StreamChannel.Instance) outputs[0]).accept(stream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Stream<Record> createStream(String path) {
        Function<GenericRecord, Record> parser = this::parseRecord;
        return readParquetFile(path).map(parser);
    }

private Stream<GenericRecord> readParquetFile(String path) {
    try {
        Path parquetPath = new Path(path);
        ParquetReader<GenericRecord> reader;
        if (this.schema != null) {
          Configuration configuration = new Configuration();
          configuration.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, schema.toString());
           reader = AvroParquetReader
              .<GenericRecord>builder(parquetPath)
              .withConf(configuration)
              .build();
        } else {
          reader = new AvroParquetReader<>(parquetPath);
        }

        Iterator<GenericRecord> recordIterator = createRecordIterator(reader);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recordIterator, 0), false);
    } catch (IOException e) {
        throw new WayangException(String.format("Error while reading Parquet file %s.", path), e);
    }
}

    private Iterator<GenericRecord> createRecordIterator(ParquetReader<GenericRecord> reader) {
        return new Iterator<GenericRecord>() {
            private GenericRecord nextRecord;

            {
                advance();
            }

            private void advance() {
                try {
                    this.nextRecord = reader.read();
                } catch (IOException e) {
                    this.nextRecord = null;
                    throw new WayangException("Error reading next record from Parquet file.", e);
                }
            }

            @Override
            public boolean hasNext() {
                return this.nextRecord != null;
            }

            @Override
            public GenericRecord next() {
                GenericRecord current = this.nextRecord;
                advance();
                return current;
            }
        };
    }

    private Record parseRecord(GenericRecord record) {
        try {
            Object[] values = new Object[record.getSchema().getFields().size()];
            int i = 0;
            for (var field : record.getSchema().getFields()) {
                values[i++] = record.get(field.name());
            }
            return new Record(values);
        } catch (Exception e) {
            throw new WayangException("Error parsing GenericRecord.", e);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
