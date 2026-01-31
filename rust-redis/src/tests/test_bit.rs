use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_setbit_getbit() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. SETBIT on new key
    // SETBIT mykey 7 1 -> returns 0
    let res = run_cmd(vec!["SETBIT", "mykey", "7", "1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // GETBIT mykey 7 -> 1
    let res = run_cmd(vec!["GETBIT", "mykey", "7"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 2. SETBIT on existing key, same bit
    // SETBIT mykey 7 1 -> returns 1
    let res = run_cmd(vec!["SETBIT", "mykey", "7", "1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 3. SETBIT on existing key, change bit
    // SETBIT mykey 7 0 -> returns 1
    let res = run_cmd(vec!["SETBIT", "mykey", "7", "0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));
    
    // GETBIT mykey 7 -> 0
    let res = run_cmd(vec!["GETBIT", "mykey", "7"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 4. SETBIT with large offset (padding)
    // SETBIT mykey 15 1 -> returns 0. Byte offset 1, bit 7.
    let res = run_cmd(vec!["SETBIT", "mykey", "15", "1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // GET mykey -> should be two bytes: \x00\x01 (since we set bit 7 of byte 0 to 0, and bit 7 of byte 1 to 1)
    let res = run_cmd(vec!["GET", "mykey"], &mut conn_ctx, &server_ctx).await;
    if let Resp::BulkString(Some(b)) = res {
        assert_eq!(b.len(), 2);
        assert_eq!(b[0], 0);
        assert_eq!(b[1], 1);
    } else { panic!("Expected BulkString"); }

    // 5. GETBIT out of range -> 0
    let res = run_cmd(vec!["GETBIT", "mykey", "100"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
}

#[tokio::test]
async fn test_bitcount() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["SET", "mykey", "foobar"], &mut conn_ctx, &server_ctx).await;
    
    // BITCOUNT mykey -> 26
    let res = run_cmd(vec!["BITCOUNT", "mykey"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(26));

    // BITCOUNT mykey 0 0 -> 4 (byte 0 is 'f' = 01100110, has 4 ones)
    let res = run_cmd(vec!["BITCOUNT", "mykey", "0", "0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(4));

    // BITCOUNT mykey 1 1 -> 6 (byte 1 is 'o' = 01101111, has 6 ones)
    let res = run_cmd(vec!["BITCOUNT", "mykey", "1", "1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(6));

    // BITCOUNT mykey 0 0 BIT -> 0 (bit 0 is 0)
    let res = run_cmd(vec!["BITCOUNT", "mykey", "0", "0", "BIT"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
}

#[tokio::test]
async fn test_bitpos() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["SETBIT", "mykey", "7", "1"], &mut conn_ctx, &server_ctx).await;
    
    // BITPOS mykey 1 -> 7
    let res = run_cmd(vec!["BITPOS", "mykey", "1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(7));

    // BITPOS mykey 0 -> 0
    let res = run_cmd(vec!["BITPOS", "mykey", "0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // Set some bits to 1 to test BITPOS 0
    // We want a string like \xff\xf0\x00
    // Byte 0: 11111111
    // Byte 1: 11110000
    // Byte 2: 00000000
    for i in 0..12 {
        run_cmd(vec!["SETBIT", "mykey", &i.to_string(), "1"], &mut conn_ctx, &server_ctx).await;
    }
    // BITPOS mykey 0 -> 12
    let res = run_cmd(vec!["BITPOS", "mykey", "0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(12));
}

#[tokio::test]
async fn test_bitop() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["SET", "key1", "foobar"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["SET", "key2", "abcdef"], &mut conn_ctx, &server_ctx).await;
    
    // BITOP AND dest key1 key2
    run_cmd(vec!["BITOP", "AND", "dest", "key1", "key2"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["GET", "dest"], &mut conn_ctx, &server_ctx).await;
    if let Resp::BulkString(Some(b)) = res {
        let mut expected = Vec::new();
        let k1 = b"foobar";
        let k2 = b"abcdef";
        for i in 0..6 { expected.push(k1[i] & k2[i]); }
        assert_eq!(b.as_ref(), expected.as_slice());
    } else { panic!(); }
}

#[tokio::test]
async fn test_bitfield() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // BITFIELD mykey SET i8 0 100 GET i8 0
    let res = run_cmd(vec!["BITFIELD", "mykey", "SET", "i8", "0", "100", "GET", "i8", "0"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], Resp::Integer(0)); // old value
        assert_eq!(arr[1], Resp::Integer(100)); // get value
    } else { panic!(); }

    // BITFIELD mykey INCRBY i8 0 30
    let res = run_cmd(vec!["BITFIELD", "mykey", "INCRBY", "i8", "0", "30"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr[0], Resp::Integer(-126)); // 100 + 30 = 130, wraps to -126 in i8
    } else { panic!(); }

    // Test SAT overflow
    run_cmd(vec!["BITFIELD", "mykey", "SET", "i8", "0", "120"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["BITFIELD", "mykey", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "10"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr[0], Resp::Integer(127)); // saturated to max i8
    } else { panic!(); }
}

#[tokio::test]
async fn test_setbit_errors() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Wrong type
    run_cmd(vec!["HSET", "myhash", "f", "v"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SETBIT", "myhash", "0", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE"),
    }

    // Invalid offset
    let res = run_cmd(vec!["SETBIT", "k", "abc", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("bit offset is not an integer")),
        _ => panic!("Expected error"),
    }

    // Invalid bit value
    let res = run_cmd(vec!["SETBIT", "k", "0", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("bit is not an integer")),
        _ => panic!("Expected error"),
    }
}
