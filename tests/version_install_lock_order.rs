use fjall::{Database, KeyspaceCreateOptions};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;
use test_log::test;

const RUN_TIME: Duration = Duration::from_secs(3);
const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(120);

/// Version installs take the journal lock (https://github.com/fjall-rs/fjall/issues/333).
///
/// Runs everything that installs a version or takes the journal lock at the same time:
/// batches, single writes, flushes, compactions, ingestions, `clear`,
/// keyspace creation and deletion. None of them may wait for each other in a cycle.
#[test]
fn version_install_lock_order() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).worker_threads(4).open()?;

    let small_memtables = || KeyspaceCreateOptions::default().max_memtable_size(64_000);
    let a = db.keyspace("a", small_memtables)?;
    let b = db.keyspace("b", small_memtables)?;
    let c = db.keyspace("c", small_memtables)?;
    let d = db.keyspace("d", small_memtables)?;

    let (db, a, b, c, d) = (&db, &a, &b, &c, &d);

    type Job<'a> = Box<dyn Fn() -> fjall::Result<()> + Send + Sync + 'a>;

    let jobs: Vec<Job<'_>> = vec![
        // Batches over two keyspaces
        Box::new(move || {
            let mut batch = db.batch();
            for i in 0..100u64 {
                batch.insert(a, i.to_be_bytes(), [0; 100]);
                batch.insert(b, i.to_be_bytes(), [0; 100]);
            }
            batch.commit()
        }),
        // Single writes, flushed by the workers while `d` is ingested into
        Box::new(move || {
            b.insert("single", [1; 100])?;
            d.insert("single", [1; 100])
        }),
        // Flushes and compactions
        Box::new(move || {
            a.rotate_memtable()?;
            b.major_compact()
        }),
        // Ingestions (flush the active memtable under the journal lock)
        //
        // NOTE: Every ingestion adds L0 runs, compact them away so writes to `d`
        // are not halted (with all workers idle) before the test ends
        Box::new(move || {
            let mut ingestion = d.start_ingestion()?;
            for i in 0..100u64 {
                ingestion.write(i.to_be_bytes(), "ingested")?;
            }
            ingestion.finish()?;
            d.major_compact()
        }),
        // Clear (called with the journal lock held)
        Box::new(move || {
            c.insert("x", "x")?;
            c.clear()
        }),
        // Keyspace creation and deletion (ingest into the meta keyspace)
        Box::new(move || {
            let ks = db.keyspace("churned", KeyspaceCreateOptions::default)?;
            ks.insert("a", "a")?;
            db.delete_keyspace(ks)
        }),
    ];

    let stop = AtomicBool::new(false);
    let (done_tx, done_rx) = std::sync::mpsc::channel::<fjall::Result<()>>();

    std::thread::scope(|scope| {
        for job in &jobs {
            let (done_tx, stop) = (done_tx.clone(), &stop);

            scope.spawn(move || {
                let result = (|| {
                    while !stop.load(Relaxed) {
                        job()?;
                    }
                    Ok(())
                })();
                done_tx.send(result).ok();
            });
        }

        std::thread::sleep(RUN_TIME);
        stop.store(true, Relaxed);

        for _ in 0..jobs.len() {
            match done_rx.recv_timeout(DEADLOCK_TIMEOUT) {
                Ok(result) => result.expect("job failed"),
                Err(e) => {
                    // NOTE: Do not wait for the stuck threads in `scope`
                    eprintln!("jobs did not stop within {DEADLOCK_TIMEOUT:?}, deadlock? ({e})");
                    std::process::abort();
                }
            }
        }
    });

    Ok(())
}
