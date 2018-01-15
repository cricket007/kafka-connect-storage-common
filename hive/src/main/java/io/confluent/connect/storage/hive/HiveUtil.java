/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.storage.hive;

import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Utility class for integration with Hive.
 */
public abstract class HiveUtil {

  private static final Logger log = LoggerFactory.getLogger(HiveUtil.class);

  protected String url;
  protected final HiveMetaStore hiveMetaStore;
  protected final String delim;
  protected final String topicsDir;
  protected Map<String, Object> tableParams;

  public HiveUtil(AbstractConfig connectorConfig, HiveMetaStore hiveMetaStore) {
    this.url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
    this.hiveMetaStore = hiveMetaStore;
    String delim = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.delim = delim != null ? delim :
          connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    this.topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    this.tableParams = new HashMap<>();
  }

  public abstract void createTable(String database, String tableName, Schema schema,
                                   Partitioner<FieldSchema> partitioner);

  public abstract void alterSchema(String database, String tableName, Schema schema);

  public abstract String getSerde();

  public abstract String getInputFormat();

  public abstract String getOutputFormat();

  protected void setTableParams(Table table) {
    if (tableParams != null) {
      for (Map.Entry<String, Object> entry : tableParams.entrySet()) {
        table.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
  }

  public Table newTable(String database, String table) {
    return new Table(database, hiveMetaStore.tableNameConverter(table));
  }

  public String hiveDirectoryName(String url, String topicsDir, String topic) {
    return Utils.join(Arrays.asList(url, topicsDir, topic), delim);
  }

  protected Table constructTable(
        String database,
        String tableName,
        Schema schema,
        Partitioner<FieldSchema> partitioner
  )
        throws HiveMetaStoreException {
    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.setProperty("EXTERNAL", "TRUE");
    String tablePath = hiveDirectoryName(url, topicsDir, tableName);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(getSerde());
    try {
      table.setInputFormatClass(getInputFormat());
      table.setOutputFormatClass(getOutputFormat());
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    table.setFields(columns);
    table.setPartCols(partitioner.partitionFields());
    setTableParams(table);
    return table;
  }

  public Future<Void> createHiveTable(
        ExecutorService executorService,
        final String hiveDatabase,
        final String tableName,
        final Schema schema,
        final Partitioner<FieldSchema> partitioner) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws HiveMetaStoreException {
        try {
          createTable(hiveDatabase, tableName, schema, partitioner);
        } catch (Throwable e) {
          log.error("Creating Hive table threw unexpected error", e);
        }
        return null;
      }
    });
  }

  public Future<Void> alterHiveSchema(
        ExecutorService executorService,
        final String hiveDatabase,
        final String tableName,
        final Schema schema) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws HiveMetaStoreException {
        try {
          alterSchema(hiveDatabase, tableName, schema);
        } catch (Throwable e) {
          log.error("Altering Hive schema threw unexpected error", e);
        }
        return null;
      }
    });
  }

  public Future<Void> addHivePartition(
        ExecutorService executorService,
        final String hiveDatabase,
        final String tableName,
        final String partitionPath) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          hiveMetaStore.addPartition(hiveDatabase, tableName, partitionPath);
        } catch (Throwable e) {
          log.error("Adding Hive partition threw unexpected error", e);
        }
        return null;
      }
    });
  }
}
