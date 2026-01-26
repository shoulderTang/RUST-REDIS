use crate::rax::Rax;

#[test]
fn test_rax_basic_ops() {
    let mut rax = Rax::new();
    
    // Test Insert
    assert_eq!(rax.insert(b"foo", 1), None);
    assert_eq!(rax.insert(b"bar", 2), None);
    assert_eq!(rax.insert(b"foobar", 3), None);
    
    // Test Get
    assert_eq!(rax.get(b"foo"), Some(&1));
    assert_eq!(rax.get(b"bar"), Some(&2));
    assert_eq!(rax.get(b"foobar"), Some(&3));
    assert_eq!(rax.get(b"baz"), None);
    
    // Test Overwrite
    assert_eq!(rax.insert(b"foo", 10), Some(1));
    assert_eq!(rax.get(b"foo"), Some(&10));
    
    // Test Remove
    assert_eq!(rax.remove(b"foo"), Some(10));
    assert_eq!(rax.get(b"foo"), None);
    assert_eq!(rax.get(b"foobar"), Some(&3)); // Child should persist
    
    assert_eq!(rax.remove(b"foobar"), Some(3));
    assert_eq!(rax.get(b"foobar"), None);
}

#[test]
fn test_rax_splitting() {
    let mut rax = Rax::new();
    
    // Insert "apple"
    rax.insert(b"apple", 1);
    
    // Insert "apply" -> splits "apple" at "appl", creates "e" and "y" branches
    rax.insert(b"apply", 2);
    
    assert_eq!(rax.get(b"apple"), Some(&1));
    assert_eq!(rax.get(b"apply"), Some(&2));
    assert_eq!(rax.get(b"appl"), None); // Intermediate node has no value
    
    // Insert "app" -> splits "appl" at "app"
    rax.insert(b"app", 3);
    assert_eq!(rax.get(b"app"), Some(&3));
    assert_eq!(rax.get(b"apple"), Some(&1));
    assert_eq!(rax.get(b"apply"), Some(&2));
}

#[test]
fn test_rax_compression() {
    let mut rax = Rax::new();
    
    // Insert "foo"
    rax.insert(b"foo", 1);
    // Insert "foobar"
    rax.insert(b"foobar", 2);
    
    // Tree: "foo" (val=1) -> "bar" (val=2)
    assert_eq!(rax.get(b"foo"), Some(&1));
    assert_eq!(rax.get(b"foobar"), Some(&2));
    
    // Remove "foo"
    // Tree should compress: "foobar" (val=2)
    // Wait, implementation details:
    // remove("foo") marks node as non-key.
    // If node has 1 child ("bar") and is not key, it should merge with child.
    // Resulting edge from root should be "foobar".
    
    rax.remove(b"foo");
    assert_eq!(rax.get(b"foo"), None);
    assert_eq!(rax.get(b"foobar"), Some(&2));
    
    // To verify compression, we'd need to inspect internals or rely on behavior.
    // If compression logic was buggy, "foobar" might not be found or double prefix.
    
    // Insert "foo" again should work
    rax.insert(b"foo", 3);
    assert_eq!(rax.get(b"foo"), Some(&3));
    assert_eq!(rax.get(b"foobar"), Some(&2));
}

#[test]
fn test_rax_random_keys() {
    let mut rax = Rax::new();
    let keys = vec![
        "romane", "romanus", "romulus", "rubens", "ruber", "rubicon", "rubicundus"
    ];
    
    for (i, key) in keys.iter().enumerate() {
        rax.insert(key.as_bytes(), i);
    }
    
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(rax.get(key.as_bytes()), Some(&i));
    }
}
