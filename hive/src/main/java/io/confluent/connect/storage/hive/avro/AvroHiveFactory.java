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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class AvroHiveFactory implements HiveFactory {

  protected final AvroData avroData;

  public AvroHiveFactory(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public HiveUtil createHiveUtil(AbstractConfig conf, HiveMetaStore hiveMetaStore) {
    return createHiveUtil((StorageSinkConnectorConfig) conf, hiveMetaStore);
  }

  public HiveUtil createHiveUtil(StorageSinkConnectorConfig conf, HiveMetaStore hiveMetaStore) {
    return createHiveUtil(conf.getStorageCommonConfig(), hiveMetaStore);
  }

  @Deprecated
  public HiveUtil createHiveUtil(StorageCommonConfig conf, HiveMetaStore hiveMetaStore) {
    return new AvroHiveUtil(conf, avroData, hiveMetaStore);
  }
}