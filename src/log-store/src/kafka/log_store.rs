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

use common_config::wal::kafka::KafkaOptions;
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendResponse, LogStore};

use crate::error::{Error, Result};
use crate::kafka::topic_client_manager::{TopicClientManager, TopicClientManagerRef};
use crate::kafka::{EntryImpl, NamespaceImpl};

#[derive(Debug)]
pub struct KafkaLogStore {
    kafka_opts: KafkaOptions,
    topic_client_manager: TopicClientManagerRef,
}

impl KafkaLogStore {
    pub async fn try_new(kafka_opts: &KafkaOptions) -> Result<Self> {
        Ok(Self {
            kafka_opts: kafka_opts.clone(),
            topic_client_manager: Arc::new(TopicClientManager::try_new(&kafka_opts).await?),
        })
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;
    type Entry = EntryImpl;
    type Namespace = NamespaceImpl;

    /// Stop components of logstore.
    async fn stop(&self) -> Result<()> {
        unimplemented!()
    }

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, e: Self::Entry) -> Result<AppendResponse> {
        unimplemented!()
    }

    /// Append a batch of entries atomically and return the offset of first entry.
    async fn append_batch(&self, e: Vec<Self::Entry>) -> Result<()> {
        unimplemented!()
    }

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        unimplemented!()
    }

    /// Create a new `Namespace`.
    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<()> {
        unimplemented!()
    }

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<()> {
        unimplemented!()
    }

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        unimplemented!()
    }

    /// Create an entry of the associate Entry type
    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        unimplemented!()
    }

    /// Create a namespace of the associate Namespace type
    fn namespace(&self, ns_id: NamespaceId) -> Self::Namespace {
        unimplemented!()
    }

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, ns: Self::Namespace, entry_id: EntryId) -> Result<()> {
        unimplemented!()
    }
}
