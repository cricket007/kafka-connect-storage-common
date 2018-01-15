/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.format.util;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.io.InputStream;

public class AvroUtils {
  public static org.apache.avro.Schema getSchema(InputStream is) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<>();
    DataFileStream<Object> streamReader = new DataFileStream<>(is, reader);
    org.apache.avro.Schema schema = streamReader.getSchema();
    streamReader.close();
    return schema;
  }
}
