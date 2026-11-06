use super::connect::XiaomiRuntime;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub struct ConnectionSession {
    runtime: Arc<XiaomiRuntime>,
    workers: Vec<JoinHandle<()>>,
}

impl ConnectionSession {
    pub fn new() -> Self {
        let runtime = Arc::new(XiaomiRuntime::new());
        runtime.running.store(true, Ordering::SeqCst);
        Self {
            runtime,
            workers: Vec::new(),
        }
    }

    pub fn runtime(&self) -> Arc<XiaomiRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn track(&mut self, worker: JoinHandle<()>) {
        self.workers.push(worker);
    }

    pub fn spawn<F>(&mut self, name: &str, work: F) -> std::io::Result<()>
    where
        F: FnOnce(Arc<XiaomiRuntime>) + Send + 'static,
    {
        let runtime = self.runtime();
        let worker = thread::Builder::new()
            .name(name.into())
            .spawn(move || work(runtime))?;
        self.track(worker);
        Ok(())
    }
}

impl Drop for ConnectionSession {
    fn drop(&mut self) {
        self.runtime.request_stop();
        self.runtime.running.store(false, Ordering::SeqCst);
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                log::warn!("Xiaomi connection worker panicked during shutdown");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn reconnect_reaps_all_workers_before_next_session() {
        let active = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            let mut session = ConnectionSession::new();
            let (ready_tx, ready_rx) = mpsc::channel();
            for _ in 0..3 {
                let active = Arc::clone(&active);
                let ready = ready_tx.clone();
                session
                    .spawn("reconnect-test", move |runtime| {
                        active.fetch_add(1, Ordering::SeqCst);
                        ready.send(()).unwrap();
                        while !runtime.should_stop() {
                            thread::park_timeout(Duration::from_millis(1));
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .unwrap();
            }
            for _ in 0..3 {
                ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            assert_eq!(active.load(Ordering::SeqCst), 3);
            session.runtime.running.store(false, Ordering::SeqCst);
            let old_runtime = session.runtime();
            drop(session);
            assert_eq!(active.load(Ordering::SeqCst), 0);
            let next = ConnectionSession::new();
            assert!(old_runtime.should_stop());
            assert!(!next.runtime.should_stop());
        }
    }
}
