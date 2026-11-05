use std::{process::Child, time::Duration};

/// Polls `GET {base}/healthz` until it answers 200, failing if `child` exits first or `within` elapses.
pub async fn wait_healthy(base: &str, child: &mut Child, within: Duration) {
    let client = reqwest::Client::new();
    let url = format!("{base}/healthz");
    let mut last = String::from("no response yet");
    let healthy = tokio::time::timeout(within, async {
        loop {
            let exited = child.try_wait().unwrap();
            assert!(exited.is_none(), "{base} exited before healthy: {exited:?}");
            match client.get(&url).send().await {
                Ok(response) if response.status() == reqwest::StatusCode::OK => return,
                Ok(response) => last = response.status().to_string(),
                Err(error) => last = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        healthy.is_ok(),
        "{url} not healthy within {within:?}, last: {last}"
    );
}
