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

use std::sync::Arc;
use std::time::Duration;

use common_config::wal::kafka::KafkaOptions;
use common_meta::wal::kafka::KafkaTopic as Topic;
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::DashMap;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::producer::aggregator::RecordAggregator;
use rskafka::client::producer::{BatchProducer, BatchProducerBuilder};
use rskafka::client::{Client, ClientBuilder};
use rskafka::BackoffConfig;
use snafu::{ensure, ResultExt};

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaPartitionClientSnafu, KafkaTopicsNotReadySnafu,
    ListCreatedKafkaTopicsSnafu, Result,
};

// There's only one partition for each topic currently.
const DEFAULT_PARTITION: i32 = 0;

#[derive(Debug)]
pub struct TopicClient {
    /// The raw client used to construct a batch producer and/or a stream consumer for a specific topic.
    pub raw_client: Arc<PartitionClient>,
    /// A producer used to buffer log entries for a specific topic.
    pub producer: Arc<BatchProducer<RecordAggregator>>,
}

pub type TopicClientRef = Arc<TopicClient>;

impl TopicClient {
    pub fn new(raw_client: Arc<PartitionClient>, kafka_opts: &KafkaOptions) -> Self {
        let record_aggregator = RecordAggregator::new(kafka_opts.max_batch_size);
        let batch_producer = BatchProducerBuilder::new(raw_client.clone())
            .with_compression(kafka_opts.compression.clone().into())
            .with_linger(kafka_opts.linger)
            .build(record_aggregator);

        Self {
            raw_client,
            producer: Arc::new(batch_producer),
        }
    }
}

#[derive(Debug)]
pub struct TopicClientManager {
    opts: KafkaOptions,
    client_factory: Client,
    topic_client_pool: DashMap<Topic, TopicClientRef>,
}

pub type TopicClientManagerRef = Arc<TopicClientManager>;

impl TopicClientManager {
    pub async fn try_new(kafka_opts: &KafkaOptions) -> Result<Self> {
        let backoff_config = BackoffConfig {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(8),
            base: 2.,
            deadline: Some(Duration::from_secs(30)),
        };

        let kafka_client = ClientBuilder::new(kafka_opts.broker_endpoints.clone())
            .backoff_config(backoff_config)
            .build()
            .await
            .context(BuildKafkaClientSnafu {
                broker_endpoints: kafka_opts.broker_endpoints.clone(),
            })?;

        // Expects all topics have been created beforehand.
        ensure!(
            check_topics_ready(&kafka_client, kafka_opts).await?,
            KafkaTopicsNotReadySnafu
        );

        Ok(Self {
            opts: kafka_opts.clone(),
            client_factory: kafka_client,
            topic_client_pool: DashMap::with_capacity(kafka_opts.num_topics),
        })
    }

    pub async fn get_or_insert(&self, topic: &str) -> Result<TopicClientRef> {
        match self.topic_client_pool.entry(topic.to_string()) {
            DashMapEntry::Occupied(entry) => Ok(entry.get().clone()),
            DashMapEntry::Vacant(entry) => {
                let topic_client = self.try_create_topic_client(topic).await?;
                Ok(entry.insert(topic_client).clone())
            }
        }
    }

    async fn try_create_topic_client(&self, topic: &str) -> Result<TopicClientRef> {
        let raw_client = self
            .client_factory
            .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
            .await
            .context(BuildKafkaPartitionClientSnafu {
                topic,
                partition: DEFAULT_PARTITION,
            })
            .map(Arc::new)?;

        let topic_client = TopicClient::new(raw_client, &self.opts);
        Ok(Arc::new(topic_client))
    }
}

// TODO(niebayes): add backoff on listing topics.
async fn check_topics_ready(kafka_client: &Client, opts: &KafkaOptions) -> Result<bool> {
    let expected_topics = (0..opts.num_topics)
        .map(|topic_id| format!("{}_{}", opts.topic_name_prefix, topic_id))
        .zip(vec![DEFAULT_PARTITION; opts.num_topics])
        .collect::<Vec<_>>();

    let actual_topics = kafka_client
        .list_topics()
        .await
        .context(ListCreatedKafkaTopicsSnafu {
            broker_endpoints: opts.broker_endpoints.clone(),
        })?;

    if expected_topics.len() == actual_topics.len() {
        let matched = expected_topics.into_iter().zip(actual_topics).all(
            |((topic_name, partition), topic)| {
                topic.name == topic_name
                    && topic.partitions.len() == 1
                    && topic.partitions.first().unwrap() == &partition
            },
        );
        Ok(matched)
    } else {
        Ok(false)
    }
}
