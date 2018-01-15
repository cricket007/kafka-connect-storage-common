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

package io.confluent.connect.storage.hive.avro;

import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;

import java.util.List;

public class AvroHiveUtil extends HiveUtil {

  protected static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  protected static final String AVRO_INPUT_FORMAT =
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  protected static final String AVRO_OUTPUT_FORMAT =
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
  protected static final String AVRO_SCHEMA_LITERAL =
        AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName();

  protected final AvroData avroData;

  public AvroHiveUtil(
        StorageCommonConfig conf, AvroData avroData, HiveMetaStore
        hiveMetaStore
  ) {
    super(conf, hiveMetaStore);
    this.avroData = avroData;
  }

  @Override
  public void createTable(String database, String tableName, Schema schema,
                          Partitioner<FieldSchema> partitioner)
        throws HiveMetaStoreException {
    tableParams.put(AVRO_SCHEMA_LITERAL, avroData.fromConnectSchema(schema));
    Table table = constructTable(database, tableName, schema, partitioner);
    hiveMetaStore.createTable(table);
  }

  @Override
  public void alterSchema(
        String database,
        String tableName,
        Schema schema
  ) throws HiveMetaStoreException {
    tableParams.put(AVRO_SCHEMA_LITERAL, avroData.fromConnectSchema(schema));
    Table table = hiveMetaStore.getTable(database, tableName);
    setTableParams(table);
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    table.setFields(columns);
    hiveMetaStore.alterTable(table);
  }

  @Override
  public String getSerde() {
    return AVRO_SERDE;
  }

  @Override
  public String getInputFormat() {
    return AVRO_INPUT_FORMAT;
  }

  @Override
  public String getOutputFormat() {
    return AVRO_OUTPUT_FORMAT;
  }
}