use crate::cmd::ConnectionContext;
use crate::cmd::{Command, Resp};

pub fn asking(_items: &[Resp], conn_ctx: &mut ConnectionContext) -> Resp {
    conn_ctx.asking = true;
    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}
