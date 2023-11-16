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

use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::{Id as NamespaceId, Namespace};

use crate::error::Error;

pub struct EntryImpl {}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = NamespaceImpl;

    fn data(&self) -> &[u8] {
        unimplemented!()
    }

    fn id(&self) -> EntryId {
        unimplemented!()
    }

    fn namespace(&self) -> Self::Namespace {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct NamespaceImpl {}

impl Namespace for NamespaceImpl {
    fn id(&self) -> NamespaceId {
        unimplemented!()
    }
}
