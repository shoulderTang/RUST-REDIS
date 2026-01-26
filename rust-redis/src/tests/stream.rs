use crate::stream::{Stream, StreamID};
use bytes::Bytes;

#[test]
fn test_stream_id_ordering() {
    let id1 = StreamID::new(1000, 0);
    let id2 = StreamID::new(1000, 1);
    let id3 = StreamID::new(1001, 0);

    assert!(id1 < id2);
    assert!(id2 < id3);
    assert!(id1 < id3);
}

#[test]
fn test_stream_basic_ops() {
    let mut stream = Stream::new();
    
    // Add first entry 1000-0
    let id1 = StreamID::new(1000, 0);
    let fields1 = vec![
        (Bytes::from("name"), Bytes::from("foo")),
        (Bytes::from("age"), Bytes::from("10")),
    ];
    assert!(stream.insert(id1, fields1.clone()).is_ok());
    assert_eq!(stream.len(), 1);
    assert_eq!(stream.last_id, id1);

    // Add second entry 1000-1
    let id2 = StreamID::new(1000, 1);
    let fields2 = vec![
        (Bytes::from("name"), Bytes::from("bar")),
    ];
    assert!(stream.insert(id2, fields2.clone()).is_ok());
    assert_eq!(stream.len(), 2);
    assert_eq!(stream.last_id, id2);

    // Add invalid entry (smaller ID)
    let id3 = StreamID::new(999, 0);
    let fields3 = vec![];
    assert!(stream.insert(id3, fields3).is_err());
    
    // Add invalid entry (same ID)
    assert!(stream.insert(id2, vec![]).is_err());
    
    // Check content
    let entry1 = stream.get(&id1).unwrap();
    assert_eq!(entry1.fields, fields1);
    
    let entry2 = stream.get(&id2).unwrap();
    assert_eq!(entry2.fields, fields2);
}

#[test]
fn test_stream_id_to_bytes() {
    let id = StreamID::new(0x1234567890ABCDEF, 0xFEDCBA0987654321);
    let bytes = id.to_be_bytes();
    
    assert_eq!(bytes[0..8], 0x1234567890ABCDEFu64.to_be_bytes());
    assert_eq!(bytes[8..16], 0xFEDCBA0987654321u64.to_be_bytes());
}
