// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use rskafka::client::partition::Compression as RsKafkaCompression;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    NoCompression,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl From<Compression> for RsKafkaCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::NoCompression => RsKafkaCompression::NoCompression,
            Compression::Gzip => RsKafkaCompression::Gzip,
            Compression::Lz4 => RsKafkaCompression::Lz4,
            Compression::Snappy => RsKafkaCompression::Snappy,
            Compression::Zstd => RsKafkaCompression::Zstd,
        }
    }
}

// TODO(niebayes): update config file accordingly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaOptions {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// Number of topics shall be created beforehand.
    pub num_topics: usize,
    /// Topic name prefix.
    pub topic_name_prefix: String,
    /// Number of partitions per topic.
    pub num_partitions: i32,
    /// The compression algorithm used to compress log entries.
    pub compression: Compression,
    /// The maximum log size an rskakfa batch producer could buffer.
    pub max_batch_size: usize,
    /// The linger duration of an rskafka batch producer.
    pub linger: Duration,
    /// The maximum amount of time (in milliseconds) to wait for Kafka records before returning.
    pub max_wait_time: i32,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 64,
            topic_name_prefix: "gt_kafka_topic".to_string(),
            num_partitions: 1,
            compression: Compression::NoCompression,
            max_batch_size: 4 * 1024 * 1024,    // 4MB.
            linger: Duration::from_millis(200), // 200ms.
            max_wait_time: 100,                 // 100ms.
        }
    }
}
