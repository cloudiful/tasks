//! The wake up channel of the queues.
//!
//! An enqueue notifies the fixed channel `cloudiful_tasks_ready` after its
//! transaction committed. The notification is a hint: it carries no payload
//! and it may be lost, so a worker also polls its queue.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

/// The channel that wakes the workers.
pub const WAKE_CHANNEL: &str = "cloudiful_tasks_ready";

/// The delay before the listener connects again.
const RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// The signal that a task became ready.
#[derive(Debug, Clone, Default)]
pub struct Wakeup {
    notify: Arc<Notify>,
}

impl Wakeup {
    /// Builds a wake up signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes the waiting workers.
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Waits for a wake up.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Listens for the wake up channel.
///
/// The listener reconnects when its session breaks. A lost notification only
/// delays the next claim, the polling of the queue covers the gap.
pub struct WakeListener {
    pool: PgPool,
}

impl WakeListener {
    /// Builds a listener over a pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Listens until the shutdown is requested.
    pub async fn run(self, wakeup: Wakeup, shutdown: CancellationToken) -> Result<()> {
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            if self.listen_once(&wakeup, &shutdown).await.is_ok() {
                return Ok(());
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(RECONNECT_DELAY) => {}
            }
        }
    }

    async fn listen_once(&self, wakeup: &Wakeup, shutdown: &CancellationToken) -> Result<()> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(WAKE_CHANNEL).await?;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                notification = listener.recv() => {
                    notification?;
                    wakeup.wake();
                }
            }
        }
    }
}
