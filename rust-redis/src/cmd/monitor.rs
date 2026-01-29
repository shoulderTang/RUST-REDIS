use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;

pub fn monitor(conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> (Resp, Option<Resp>) {
    if let Some(sender) = &conn_ctx.msg_sender {
        server_ctx.monitors.insert(conn_ctx.id, sender.clone());
        (Resp::SimpleString(Bytes::from("OK")), None)
    } else {
        (Resp::Error("ERR no message sender available".to_string()), None)
    }
}
