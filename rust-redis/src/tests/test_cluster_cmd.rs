#[cfg(test)]
mod tests {
    use crate::cluster::{ClusterState, NodeId, NodeRole};
    use crate::resp::Resp;
    use crate::tests::helper::{
        create_connection_context, create_server_context, create_server_context_with_cluster,
        run_cmd,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn test_cluster_gating_when_disabled() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(vec!["CLUSTER", "NODES"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Error(e) => assert!(e.contains("cluster support disabled")),
            _ => panic!("Expected error when cluster disabled, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_basic_myid_and_nodes() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(vec!["CLUSTER", "MYID"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::BulkString(Some(b)) => assert_eq!(b.len(), 40),
            _ => panic!("Expected MYID BulkString, got {:?}", res),
        }

        let res = run_cmd(vec!["CLUSTER", "NODES"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::BulkString(Some(_)) => {}
            _ => panic!("Expected NODES BulkString, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_addslots_and_slots() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(
            vec!["CLUSTER", "ADDSLOTS", "0", "1", "2"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        let res = run_cmd(vec!["CLUSTER", "SLOTS"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert!(arr.len() >= 1),
            _ => panic!("Expected SLOTS Array, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_keyslot_and_count_getkeys() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        // Compute slot for k1 and assign it
        let res = run_cmd(vec!["CLUSTER", "KEYSLOT", "k1"], &mut conn_ctx, &server_ctx).await;
        let slot = match res {
            Resp::Integer(n) => n,
            _ => panic!("Expected Integer for KEYSLOT, got {:?}", res),
        };
        run_cmd(
            vec!["CLUSTER", "ADDSLOTS", &slot.to_string()],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;

        // Insert key in the assigned slot
        run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &server_ctx).await;

        // Count keys in that slot should be >= 1
        let res = run_cmd(
            vec!["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        match res {
            Resp::Integer(n) => assert!(n >= 1),
            _ => panic!("Expected Integer for COUNTKEYSINSLOT, got {:?}", res),
        }

        // Get some keys in that slot
        let res = run_cmd(
            vec!["CLUSTER", "GETKEYSINSLOT", &slot.to_string(), "10"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        match res {
            Resp::Array(Some(_)) => {}
            _ => panic!("Expected Array for GETKEYSINSLOT, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_meet_forget_and_setslot() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        // MEET a new node
        let res = run_cmd(
            vec!["CLUSTER", "MEET", "1.2.3.4", "7000"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // SETSLOT STABLE (arbitrary slot)
        let res = run_cmd(
            vec!["CLUSTER", "SETSLOT", "123", "STABLE"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // FORGET the met node (has no slots so should be OK)
        let res = run_cmd(
            vec!["CLUSTER", "FORGET", "1.2.3.4:7000"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    }

    #[tokio::test]
    async fn test_cluster_topology_merge_replaces_placeholder_by_addr() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(
            vec!["CLUSTER", "MEET", "127.0.0.1", "7001"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        let remote_text = {
            let mut remote = ClusterState::new(
                NodeId("remote-node-id".to_string()),
                "127.0.0.1".to_string(),
                7001,
            );
            let rid = remote.myself.clone();
            remote.add_slots(&rid, &[0, 1, 2]).unwrap();
            let mut s = remote.nodes_overview().join("\n");
            s.push('\n');
            s
        };

        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            st.merge_nodes_overview_text(&remote_text).unwrap();
            assert!(st.nodes.contains_key(&NodeId("remote-node-id".to_string())));
            assert!(!st.nodes.contains_key(&NodeId("127.0.0.1:7001".to_string())));
            assert_eq!(st.slots[0], Some(NodeId("remote-node-id".to_string())));
        }
    }

    #[tokio::test]
    async fn test_cluster_topology_merge_tracks_replica_relationship() {
        let server_ctx = create_server_context_with_cluster();

        let remote_text = {
            let master_id = NodeId("master-1".to_string());
            let mut remote = ClusterState::new(master_id.clone(), "127.0.0.1".to_string(), 7001);
            remote
                .add_node(
                    NodeId("replica-1".to_string()),
                    "127.0.0.1".to_string(),
                    7002,
                    NodeRole::Replica,
                    Some(master_id.clone()),
                )
                .unwrap();
            let mut s = remote.nodes_overview().join("\n");
            s.push('\n');
            s
        };

        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            st.merge_nodes_overview_text(&remote_text).unwrap();
            let rep = st.nodes.get(&NodeId("replica-1".to_string())).unwrap();
            assert_eq!(rep.role, NodeRole::Replica);
            assert_eq!(rep.master_id, Some(NodeId("master-1".to_string())));
        }
    }

    #[tokio::test]
    async fn test_cluster_failover_promotes_replica_when_master_unreachable() {
        let server_ctx = create_server_context_with_cluster();
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let master_id = NodeId("m-1".to_string());
            st.add_node(
                master_id.clone(),
                "127.0.0.1".to_string(),
                7001,
                NodeRole::Master,
                None,
            )
            .unwrap();
            st.add_slots(&master_id, &[0, 1, 2]).unwrap();
            let my = st.myself.clone();
            if let Some(me) = st.nodes.get_mut(&my) {
                me.role = NodeRole::Replica;
                me.master_id = Some(master_id.clone());
            }
            st.last_ok_ms.insert(master_id.clone(), 0);
            let my_id = st.myself.clone();
            st.scan_and_failover(0, |n| n.id == my_id);
            let me = st.nodes.get(&st.myself).unwrap();
            assert_eq!(me.role, NodeRole::Master);
            for s in 0..=2 {
                assert_eq!(st.slots[s as usize], Some(st.myself.clone()));
            }
        }
        let mut conn_ctx = create_connection_context();
        let _ = run_cmd(vec!["SET", "{0}k", "v"], &mut conn_ctx, &server_ctx).await;
    }

    #[tokio::test]
    async fn test_cluster_moved_error_when_slot_owned_by_remote() {
        let server_ctx = create_server_context_with_cluster();
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let remote_id = NodeId("remote-1".to_string());
            st.add_node(
                remote_id.clone(),
                "10.0.0.1".to_string(),
                7001,
                NodeRole::Master,
                None,
            )
            .unwrap();
            let slot = ClusterState::key_slot("moved_key");
            st.add_slots(&remote_id, &[slot]).unwrap();
        }
        let mut conn_ctx = create_connection_context();
        let res = run_cmd(vec!["SET", "moved_key", "v"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Error(e) => {
                assert!(e.starts_with("MOVED "));
                assert!(e.contains("10.0.0.1:7001"));
            }
            _ => panic!("Expected MOVED error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_crossslot_error_for_multi_key() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();
        let res = run_cmd(
            vec!["MSET", "k1", "v1", "k2", "v2"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        match res {
            Resp::Error(e) => assert!(e.starts_with("CROSSSLOT")),
            _ => panic!("Expected CROSSSLOT error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_migrating_returns_ask() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();
        let key = "migrate_key";
        let slot = ClusterState::key_slot(key);
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let remote_id = NodeId("1.1.1.1:7001".to_string());
            st.add_node(
                remote_id.clone(),
                "1.1.1.1".to_string(),
                7001,
                NodeRole::Master,
                None,
            )
            .unwrap();
            st.slot_state[slot as usize] = crate::cluster::SlotState::Migrating { to: remote_id };
        }
        let res = run_cmd(vec!["SET", key, "v"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Error(e) => {
                assert!(e.starts_with("ASK "));
                assert!(e.contains("1.1.1.1:7001"));
            }
            _ => panic!("Expected ASK error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_importing_requires_asking() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();
        let key = "import_key";
        let slot = ClusterState::key_slot(key);
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let from_id = NodeId("2.2.2.2:7002".to_string());
            st.add_node(
                from_id.clone(),
                "2.2.2.2".to_string(),
                7002,
                NodeRole::Master,
                None,
            )
            .unwrap();
            st.slot_state[slot as usize] = crate::cluster::SlotState::Importing { from: from_id };
        }
        let res = run_cmd(vec!["SET", key, "v"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Error(e) => {
                assert!(e.starts_with("ASK "));
                assert!(e.contains("2.2.2.2:7002"));
            }
            _ => panic!("Expected ASK error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_importing_with_asking_succeeds() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();
        let key = "{42}import";
        let slot = ClusterState::key_slot(key);
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let from_id = NodeId("3.3.3.3:7003".to_string());
            st.add_node(
                from_id.clone(),
                "3.3.3.3".to_string(),
                7003,
                NodeRole::Master,
                None,
            )
            .unwrap();
            let my = st.myself.clone();
            st.add_slots(&my, &[slot]).unwrap();
            st.slot_state[slot as usize] = crate::cluster::SlotState::Importing { from: from_id };
        }
        let _ = run_cmd(vec!["ASKING"], &mut conn_ctx, &server_ctx).await;
        let res = run_cmd(vec!["SET", key, "v"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    }

    #[tokio::test]
    async fn test_cluster_slots_includes_replicas() {
        let server_ctx = create_server_context_with_cluster();
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            let master_id = NodeId("ms-1".to_string());
            st.add_node(
                master_id.clone(),
                "127.0.0.1".to_string(),
                7001,
                NodeRole::Master,
                None,
            )
            .unwrap();
            st.add_slots(&master_id, &[0]).unwrap();
            let my = st.myself.clone();
            if let Some(me) = st.nodes.get_mut(&my) {
                me.role = NodeRole::Replica;
                me.master_id = Some(master_id.clone());
            }
        }
        let mut conn_ctx = create_connection_context();
        let res = run_cmd(vec!["CLUSTER", "SLOTS"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(entries)) => {
                let mut has_replica = false;
                for e in entries {
                    if let Resp::Array(Some(parts)) = e {
                        if parts.len() >= 4 {
                            if let Resp::Integer(start) = parts[0] {
                                if start == 0 {
                                    if let Resp::Array(Some(_master)) = &parts[2] {
                                        if let Resp::Array(Some(replicas)) = &parts[3] {
                                            if !replicas.is_empty() {
                                                has_replica = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                assert!(has_replica);
            }
            _ => panic!("Expected SLOTS array, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_flushslots_removes_all_slots() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        // Add some slots first
        let res = run_cmd(
            vec!["CLUSTER", "ADDSLOTS", "0", "1", "2"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // Verify slots are assigned (merged into 1 range)
        {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            let my_id = st.myself.clone();
            let node = st.nodes.get(&my_id).unwrap();
            assert_eq!(node.slots.len(), 1);
            assert_eq!(node.slots[0].start, 0);
            assert_eq!(node.slots[0].end, 2);
        }

        // Flush all slots
        let res = run_cmd(vec!["CLUSTER", "FLUSHSLOTS"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // Verify slots are removed
        {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            let my_id = st.myself.clone();
            let node = st.nodes.get(&my_id).unwrap();
            assert_eq!(node.slots.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_cluster_bumpepoch_increments_epoch() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let initial_epoch = {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            st.current_epoch
        };

        // Bump epoch
        let res = run_cmd(vec!["CLUSTER", "BUMPEPOCH"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // Verify epoch was incremented
        {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            assert_eq!(st.current_epoch, initial_epoch + 1);
        }
    }

    #[tokio::test]
    async fn test_cluster_saveconfig_returns_ok() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(vec!["CLUSTER", "SAVECONFIG"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    }

    #[tokio::test]
    async fn test_cluster_set_config_epoch_sets_initial_epoch() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        // Reset epoch to 0 first
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            st.current_epoch = 0;
        }

        // Set config epoch
        let res = run_cmd(
            vec!["CLUSTER", "SET-CONFIG-EPOCH", "123"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        // Verify epoch was set
        {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            assert_eq!(st.current_epoch, 123);
        }

        // Try to set again - should fail
        let res = run_cmd(
            vec!["CLUSTER", "SET-CONFIG-EPOCH", "456"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        match res {
            Resp::Error(msg) => assert!(msg.contains("Node already has a config epoch")),
            _ => panic!("Expected error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_links_returns_empty_array() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let res = run_cmd(vec!["CLUSTER", "LINKS"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(links)) => assert_eq!(links.len(), 0),
            _ => panic!("Expected empty array, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_myshardid_returns_node_id() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();

        let expected_id = {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            st.myself.0.clone()
        };

        let res = run_cmd(vec!["CLUSTER", "MYSHARDID"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::BulkString(Some(id)) => assert_eq!(id, Bytes::from(expected_id)),
            _ => panic!("Expected bulk string, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_cluster_saveconfig_persists_to_node_conf() {
        let server_ctx = create_server_context_with_cluster();
        let mut conn_ctx = create_connection_context();
        let _ = run_cmd(
            vec!["CLUSTER", "ADDSLOTS", "0", "1", "2"],
            &mut conn_ctx,
            &server_ctx,
        )
        .await;
        let _ = run_cmd(vec!["CLUSTER", "BUMPEPOCH"], &mut conn_ctx, &server_ctx).await;
        let _ = run_cmd(vec!["CLUSTER", "BUMPEPOCH"], &mut conn_ctx, &server_ctx).await;
        let _ = run_cmd(vec!["CLUSTER", "BUMPEPOCH"], &mut conn_ctx, &server_ctx).await;
        let res = run_cmd(vec!["CLUSTER", "SAVECONFIG"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
        let path = std::path::Path::new(&server_ctx.config.dir)
            .join(&server_ctx.config.cluster_config_file);
        assert!(path.exists());
        let text = std::fs::read_to_string(&path).unwrap();
        let mut tmp = {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            crate::cluster::ClusterState::new(
                st.myself.clone(),
                server_ctx.config.bind.clone(),
                server_ctx.config.port,
            )
        };
        tmp.load_config_text(&text, &server_ctx.config.bind, server_ctx.config.port)
            .unwrap();
        assert_eq!(tmp.current_epoch, 3);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&server_ctx.config.dir));
    }

    #[tokio::test]
    async fn test_cluster_load_config_text_builds_state() {
        let server_ctx = create_server_context_with_cluster();
        let text = {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            let my_id = st.myself.0.clone();
            let addr = format!("{}:{}", server_ctx.config.bind, server_ctx.config.port);
            format!(
                "currentEpoch 5\nmyself {my_id}\n{my_id} {addr} master 5 0-2\nn2 127.0.0.1:7001 master 4 3-4\n"
            )
        };
        {
            let mut st = server_ctx.cluster_ctx.state.write().unwrap();
            st.load_config_text(&text, &server_ctx.config.bind, server_ctx.config.port)
                .unwrap();
        }
        {
            let st = server_ctx.cluster_ctx.state.read().unwrap();
            assert_eq!(st.current_epoch, 5);
            let me = st.myself.clone();
            assert!(st.nodes.contains_key(&me));
            assert_eq!(st.slots[0], Some(me.clone()));
            assert_eq!(st.slots[2], Some(me.clone()));
        }
    }
}
