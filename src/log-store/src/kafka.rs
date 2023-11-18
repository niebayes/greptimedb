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

pub mod log_store;
mod topic_client_manager;
use std::collections::BTreeMap;

use common_meta::wal::kafka::KafkaTopic as Topic;
use rskafka::record::Record;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};
use store_api::storage::RegionId;

use crate::error::{
    DeserEntryIdSnafu, DeserRegionIdSnafu, DeserTopicSnafu, Error, MissingEntryIdSnafu,
    MissingRecordValueSnafu, MissingRegionIdSnafu, MissingTopicSnafu, Result, SerEntryIdSnafu,
    SerRegionIdSnafu, SerTopicSnafu,
};

const ENTRY_ID_KEY: &str = "entry_id";
const TOPIC_KEY: &str = "topic";
const REGION_ID_KEY: &str = "region_id";

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct NamespaceImpl {
    topic: Topic,
    region_id: RegionId,
}

impl NamespaceImpl {
    pub fn new(topic: Topic, region_id: RegionId) -> Self {
        Self { topic, region_id }
    }

    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    pub fn region_id(&self) -> &RegionId {
        &self.region_id
    }
}

impl Namespace for NamespaceImpl {
    fn id(&self) -> NamespaceId {
        unreachable!()
    }
}

pub struct EntryImpl {
    data: Vec<u8>,
    id: EntryId,
    pub ns: NamespaceImpl,
}

impl EntryImpl {
    pub fn new(data: Vec<u8>, entry_id: EntryId, ns: NamespaceImpl) -> Self {
        Self {
            data,
            id: entry_id,
            ns,
        }
    }
}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = NamespaceImpl;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn id(&self) -> EntryId {
        self.id
    }

    fn namespace(&self) -> Self::Namespace {
        self.ns.clone()
    }
}

impl TryInto<Record> for EntryImpl {
    type Error = Error;

    fn try_into(self) -> Result<Record> {
        let entry_id = self.id;
        let topic = self.ns.topic();
        let region_id = self.ns.region_id();

        let raw_entry_id = serde_json::to_vec(&entry_id).context(SerEntryIdSnafu { entry_id })?;
        let raw_topic = serde_json::to_vec(topic).context(SerTopicSnafu { topic })?;
        let raw_region_id = serde_json::to_vec(region_id).context(SerRegionIdSnafu {
            region_id: *region_id,
        })?;

        let headers = BTreeMap::from([
            (ENTRY_ID_KEY.to_string(), raw_entry_id),
            (TOPIC_KEY.to_string(), raw_topic),
            (REGION_ID_KEY.to_string(), raw_region_id),
        ]);

        Ok(Record {
            key: None,
            value: Some(self.data),
            headers,
            timestamp: rskafka::chrono::Utc::now(),
        })
    }
}

impl TryFrom<Record> for EntryImpl {
    type Error = Error;

    fn try_from(record: Record) -> Result<Self> {
        let value = record.value.as_ref().context(MissingRecordValueSnafu {
            record: record.clone(),
        })?;

        let headers = &record.headers;
        let raw_entry_id = headers.get(REGION_ID_KEY).context(MissingEntryIdSnafu {
            record: record.clone(),
        })?;
        let raw_topic = headers.get(TOPIC_KEY).context(MissingTopicSnafu {
            record: record.clone(),
        })?;
        let raw_region_id = headers.get(REGION_ID_KEY).context(MissingRegionIdSnafu {
            record: record.clone(),
        })?;

        let entry_id = serde_json::from_slice(raw_entry_id).context(DeserEntryIdSnafu {
            record: record.clone(),
        })?;
        let topic = serde_json::from_slice(raw_topic).context(DeserTopicSnafu {
            record: record.clone(),
        })?;
        let region_id = serde_json::from_slice(raw_region_id).context(DeserRegionIdSnafu {
            record: record.clone(),
        })?;

        Ok(Self {
            data: value.clone(),
            id: entry_id,
            ns: NamespaceImpl::new(topic, region_id),
        })
    }
}
