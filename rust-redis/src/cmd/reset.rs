use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;

pub fn reset(conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    // 1. Reset DB index
    conn_ctx.db_index = 0;

    // 2. Reset Authentication (to default)
    conn_ctx.authenticated = false;
    conn_ctx.current_username = "default".to_string();

    // 3. Reset Multi state
    conn_ctx.in_multi = false;
    conn_ctx.multi_queue.clear();

    // 4. Reset Watch state
    for (db_idx, keys) in conn_ctx.watched_keys.iter() {
        for key in keys {
            if let Some(mut clients) = server_ctx.watched_clients.get_mut(&(*db_idx, key.clone())) {
                clients.remove(&conn_ctx.id);
            }
        }
    }
    conn_ctx.watched_keys.clear();
    conn_ctx.watched_keys_dirty.store(false, Ordering::SeqCst);

    // 5. Reset PubSub state (silent unsubscribe)
    for channel in &conn_ctx.subscriptions {
        if let Some(subscribers) = server_ctx.pubsub_channels.get(channel) {
            subscribers.remove(&conn_ctx.id);
        }
    }
    conn_ctx.subscriptions.clear();

    for pattern in &conn_ctx.psubscriptions {
        if let Some(subscribers) = server_ctx.pubsub_patterns.get(pattern) {
            subscribers.remove(&conn_ctx.id);
        }
    }
    conn_ctx.psubscriptions.clear();

    // 6. Reset Client Name
    if let Some(mut client_info) = server_ctx.clients.get_mut(&conn_ctx.id) {
        client_info.name = String::new();
    }

    Resp::SimpleString(Bytes::from("RESET"))
}
