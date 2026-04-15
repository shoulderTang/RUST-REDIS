use crate::resp::Resp;
use crate::tests::helper::{create_connection_context, create_server_context, run_cmd};
use bytes::Bytes;

#[tokio::test]
async fn test_replconf_listening_port() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // Test REPLCONF LISTENING-PORT command
    let res = run_cmd(
        vec!["REPLCONF", "LISTENING-PORT", "6381"],
        &mut conn_ctx,
        &server_ctx,
    )
    .await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Verify the listening port is stored correctly
    assert!(server_ctx.replica_listening_port.contains_key(&conn_ctx.id));
    let stored_port = server_ctx.replica_listening_port.get(&conn_ctx.id).unwrap();
    assert_eq!(*stored_port.value(), 6381u16);
}

#[tokio::test]
async fn test_replconf_listening_port_invalid() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // Test invalid port number
    let res = run_cmd(
        vec!["REPLCONF", "LISTENING-PORT", "99999"],
        &mut conn_ctx,
        &server_ctx,
    )
    .await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Should store 0 for invalid port
    let stored_port = server_ctx.replica_listening_port.get(&conn_ctx.id).unwrap();
    assert_eq!(*stored_port.value(), 0u16);
}

#[tokio::test]
async fn test_replconf_listening_port_zero() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // Test port 0 (should not be stored)
    let res = run_cmd(
        vec!["REPLCONF", "LISTENING-PORT", "0"],
        &mut conn_ctx,
        &server_ctx,
    )
    .await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Should not store port 0
    assert!(!server_ctx.replica_listening_port.contains_key(&conn_ctx.id));
}
