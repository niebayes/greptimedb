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

pub mod kafka;
pub mod raft_engine;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::KafkaOptions;
use crate::wal::raft_engine::RaftEngineOptions;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalProvider {
    RaftEngine,
    Kafka,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct WalOptions {
    pub provider: WalProvider,
    pub raft_engine_opts: Option<RaftEngineOptions>,
    pub kafka_opts: Option<KafkaOptions>,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            provider: WalProvider::RaftEngine,
            raft_engine_opts: Some(RaftEngineOptions::default()),
            kafka_opts: None,
        }
    }
}
