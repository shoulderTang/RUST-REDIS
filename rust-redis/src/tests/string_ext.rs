use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_setnx_setex_psetex() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SETNX key_nx val_nx -> 1 (success)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SETNX"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
        Resp::BulkString(Some(Bytes::from("val_nx"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SETNX key_nx val_nx_2 -> 0 (fail)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SETNX"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
        Resp::BulkString(Some(Bytes::from("val_nx_2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // GET key_nx -> val_nx
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_nx")),
        _ => panic!("expected BulkString(val_nx)"),
    }

    // SETEX key_ex 10 val_ex -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SETEX"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
        Resp::BulkString(Some(Bytes::from("10"))),
        Resp::BulkString(Some(Bytes::from("val_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // TTL key_ex -> > 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 10),
        _ => panic!("expected Integer(> 0)"),
    }

    // GET key_ex -> val_ex
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_ex")),
        _ => panic!("expected BulkString(val_ex)"),
    }

    // PSETEX key_pex 10000 val_pex -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PSETEX"))),
        Resp::BulkString(Some(Bytes::from("key_pex"))),
        Resp::BulkString(Some(Bytes::from("10000"))),
        Resp::BulkString(Some(Bytes::from("val_pex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // PTTL key_pex -> > 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PTTL"))),
        Resp::BulkString(Some(Bytes::from("key_pex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 10000),
        _ => panic!("expected Integer(> 0)"),
    }

    // GET key_pex -> val_pex
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_pex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_pex")),
        _ => panic!("expected BulkString(val_pex)"),
    }
}

#[tokio::test]
async fn test_getset() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // GETSET key_gs val_gs_1 -> nil (since key not exists)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETSET"))),
        Resp::BulkString(Some(Bytes::from("key_gs"))),
        Resp::BulkString(Some(Bytes::from("val_gs_1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None), got {:?}", res),
    }

    // GET key_gs -> val_gs_1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_gs"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_gs_1")),
        _ => panic!("expected BulkString(val_gs_1)"),
    }

    // GETSET key_gs val_gs_2 -> val_gs_1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETSET"))),
        Resp::BulkString(Some(Bytes::from("key_gs"))),
        Resp::BulkString(Some(Bytes::from("val_gs_2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_gs_1")),
        _ => panic!("expected BulkString(val_gs_1), got {:?}", res),
    }

    // GET key_gs -> val_gs_2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_gs"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_gs_2")),
        _ => panic!("expected BulkString(val_gs_2)"),
    }
}

#[tokio::test]
async fn test_getdel_getex() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // GETDEL on non-existent key -> nil
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETDEL"))),
        Resp::BulkString(Some(Bytes::from("key_gd"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None), got {:?}", res),
    }

    // SET key_gd val_gd
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key_gd"))),
        Resp::BulkString(Some(Bytes::from("val_gd"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GETDEL key_gd -> val_gd
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETDEL"))),
        Resp::BulkString(Some(Bytes::from("key_gd"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_gd")),
        _ => panic!("expected BulkString(val_gd)"),
    }

    // GET key_gd -> nil
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("key_gd"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // SET key_ex val_ex
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
        Resp::BulkString(Some(Bytes::from("val_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GETEX key_ex EX 10 -> val_ex
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETEX"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
        Resp::BulkString(Some(Bytes::from("EX"))),
        Resp::BulkString(Some(Bytes::from("10"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_ex")),
        _ => panic!("expected BulkString(val_ex)"),
    }

    // TTL key_ex -> > 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 10),
        _ => panic!("expected Integer(> 0)"),
    }

    // GETEX key_ex PERSIST -> val_ex
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GETEX"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
        Resp::BulkString(Some(Bytes::from("PERSIST"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("val_ex")),
        _ => panic!("expected BulkString(val_ex)"),
    }

    // TTL key_ex -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("key_ex"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }
}

#[tokio::test]
async fn test_incrbyfloat() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // INCRBYFLOAT key_float 10.5 -> 10.5
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBYFLOAT"))),
        Resp::BulkString(Some(Bytes::from("key_float"))),
        Resp::BulkString(Some(Bytes::from("10.5"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("10.5")),
        _ => panic!("expected BulkString(10.5)"),
    }

    // INCRBYFLOAT key_float 0.1 -> 10.6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBYFLOAT"))),
        Resp::BulkString(Some(Bytes::from("key_float"))),
        Resp::BulkString(Some(Bytes::from("0.1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("10.6")),
        _ => panic!("expected BulkString(10.6)"),
    }

    // INCRBYFLOAT key_float -5 -> 5.6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBYFLOAT"))),
        Resp::BulkString(Some(Bytes::from("key_float"))),
        Resp::BulkString(Some(Bytes::from("-5"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("5.6")),
        _ => panic!("expected BulkString(5.6)"),
    }

    // INCRBYFLOAT key_float 2.5e2 -> 255.6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBYFLOAT"))),
        Resp::BulkString(Some(Bytes::from("key_float"))),
        Resp::BulkString(Some(Bytes::from("2.5e2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("255.6")),
        _ => panic!("expected BulkString(255.6)"),
    }
}

#[tokio::test]
async fn test_msetnx() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // MSETNX k1 v1 k2 v2 -> 1 (all set)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSETNX"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // GET k1 -> v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("v1")),
        _ => panic!("expected BulkString(v1)"),
    }

    // MSETNX k2 v2_new k3 v3 -> 0 (k2 exists, nothing set)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSETNX"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2_new"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // GET k2 -> v2 (not v2_new)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("v2")),
        _ => panic!("expected BulkString(v2)"),
    }

    // GET k3 -> nil (not set)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }
}

#[tokio::test]
async fn test_stralgo() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // STRALGO LCS STRINGS "ohmytext" "mynewtext" -> "mytext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("mytext")),
        _ => panic!("expected BulkString(mytext), got {:?}", res),
    }

    // STRALGO LCS STRINGS "ohmytext" "mynewtext" LEN -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
        Resp::BulkString(Some(Bytes::from("LEN"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6), got {:?}", res),
    }

    // SET k1 "ohmytext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // SET k2 "mynewtext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // STRALGO LCS KEYS k1 k2 -> "mytext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("mytext")),
        _ => panic!("expected BulkString(mytext), got {:?}", res),
    }
}

#[tokio::test]
async fn test_stralgo_minmatchlen() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // STRALGO LCS STRINGS "ohmytext" "mynewtext" IDX MINMATCHLEN 4 -> matches only "text"
    // "ohmytext" vs "mynewtext"
    // LCS is "mytext" (length 6)
    // Matches: "my" (len 2), "text" (len 4)
    // MINMATCHLEN 4 should filter out "my"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
        Resp::BulkString(Some(Bytes::from("IDX"))),
        Resp::BulkString(Some(Bytes::from("MINMATCHLEN"))),
        Resp::BulkString(Some(Bytes::from("4"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // arr[0] = "matches"
            // arr[1] = matches_array
            // arr[2] = "len"
            // arr[3] = 6
            assert_eq!(arr.len(), 4);
            match &arr[1] {
                Resp::Array(Some(matches)) => {
                    assert_eq!(matches.len(), 1);
                    // Verify the match is "text" (indices 4-7 in A, 5-8 in B)
                    // item is [[4, 7], [5, 8]]
                    match &matches[0] {
                        Resp::Array(Some(item)) => {
                             // item[0] = [4, 7]
                             match &item[0] {
                                 Resp::Array(Some(range)) => {
                                     match (&range[0], &range[1]) {
                                         (Resp::Integer(s), Resp::Integer(e)) => {
                                             assert_eq!(*s, 4);
                                             assert_eq!(*e, 7);
                                         },
                                         _ => panic!("invalid range format"),
                                     }
                                 },
                                 _ => panic!("invalid range"),
                             }
                        },
                        _ => panic!("invalid match item"),
                    }
                },
                _ => panic!("expected matches array"),
            }
        },
        _ => panic!("expected Array, got {:?}", res),
    }

    // Test alias MINLEN
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
        Resp::BulkString(Some(Bytes::from("IDX"))),
        Resp::BulkString(Some(Bytes::from("MINLEN"))),
        Resp::BulkString(Some(Bytes::from("4"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
         Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            match &arr[1] {
                Resp::Array(Some(matches)) => {
                    assert_eq!(matches.len(), 1);
                },
                _ => panic!("expected matches array"),
            }
         },
         _ => panic!("expected Array"),
    }
}
