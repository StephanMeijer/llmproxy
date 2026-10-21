use std::{process::Child, process::Command, thread, time::Duration, time::Instant};

const GRACE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(20);

pub fn stop(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    if !thread::panicking() {
        let signal = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        if signal.is_ok_and(|status| status.success()) {
            let deadline = Instant::now() + GRACE;
            while Instant::now() < deadline {
                if let Ok(Some(status)) = child.try_wait() {
                    assert!(status.success(), "child failed during graceful shutdown");
                    return;
                }
                thread::sleep(POLL);
            }
        }
    }
    let killed = child.kill();
    let waited = child.wait();
    if !thread::panicking() {
        killed.unwrap();
        assert!(waited.unwrap().success(), "child required forced shutdown");
    }
}
