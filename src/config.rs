// Copyright 2026 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{collections::HashSet, sync::Arc};

/// Configuration for a Waggle protocol instance.
#[derive(Debug, Clone)]
pub struct Config {
    max_connection_queue_limit: usize,
    max_topic_subscription: usize,
    max_protobuf_size: usize,
    pub(crate) topic_whitelist: Option<Arc<HashSet<Vec<u8>>>>,
}

impl Config {
    /// The maximum number of messages queued outbound on one connection.
    ///
    /// The budget is shared by all the [`PieceSender`](crate::handler::PieceSender)s
    /// of the connection. When the connection queue is full, they reject
    /// further messages with [`SendError::Full`](crate::handler::SendError::Full).
    ///
    /// Since the budget is shared, `Full` means the peer as a whole is not
    /// keeping up, not that a single topic is full. Messages queued on a
    /// topic the peer stopped reading keep their share of the budget until
    /// the stream closes, so a stall on one topic eventually rejects sends
    /// on every topic of the connection.
    ///
    /// Defaults to 5000.
    pub fn max_connection_queue_limit(&self) -> usize {
        self.max_connection_queue_limit
    }

    /// Sets the maximum number of messages queued outbound on one connection.
    pub fn set_max_connection_queue_limit(mut self, len: usize) -> Self {
        self.max_connection_queue_limit = len;
        self
    }

    /// The maximum number of topics a single peer may subscribe to.
    ///
    /// Defaults to 50.
    pub fn max_topic_subscription(&self) -> usize {
        self.max_topic_subscription
    }

    /// Sets the maximum number of topics a single peer may subscribe to.
    pub fn set_max_topic_subscription(mut self, value: usize) -> Self {
        self.max_topic_subscription = value;
        self
    }

    /// The maximum size in bytes of a single protobuf message on the wire.
    ///
    /// Defaults to 65536 (64 KB).
    pub fn max_protobuf_size(&self) -> usize {
        self.max_protobuf_size
    }

    /// Sets the maximum size in bytes of a single protobuf message on the wire.
    pub fn set_max_protobuf_size(mut self, value: usize) -> Self {
        self.max_protobuf_size = value;
        self
    }

    /// The topic subscription whitelist.
    ///
    /// When it is `None`, all topic subscriptions are accepted.
    pub fn topic_whitelist(&self) -> Option<&HashSet<Vec<u8>>> {
        self.topic_whitelist.as_deref()
    }

    /// Sets the topic subscription whitelist.
    pub fn set_topic_whitelist(mut self, whitelist: Option<HashSet<Vec<u8>>>) -> Self {
        self.topic_whitelist = whitelist.map(Arc::new);
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connection_queue_limit: 5000,
            max_topic_subscription: 50,
            topic_whitelist: None,
            max_protobuf_size: 65536,
        }
    }
}
