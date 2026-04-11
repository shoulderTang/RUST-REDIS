use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static CACHED_MS: AtomicU64 = AtomicU64::new(0);
static CACHED_SECS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn now_ms() -> u64 {
    let v = CACHED_MS.load(Ordering::Relaxed);
    if v != 0 {
        v
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[inline]
pub fn now_secs() -> u64 {
    let v = CACHED_SECS.load(Ordering::Relaxed);
    if v != 0 {
        v
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

fn update() {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    CACHED_MS.store(d.as_millis() as u64, Ordering::Relaxed);
    CACHED_SECS.store(d.as_secs(), Ordering::Relaxed);
}

pub fn start_clock_task() {
    // Initialize immediately so the cache is valid before any key operation
    update();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            update();
        }
    });
}
