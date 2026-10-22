use llmproxy::proxy::Metrics;

const SERIES: &str = "llm3_shared_limit_conflicts_total{provider=\"anthropic\"}";
const THREADS: u64 = 8;
const CONFLICTS: u64 = 100;

#[test]
fn shared_limit_conflicts_start_at_zero() {
    // Given a fresh process metric registry.
    let metrics = Metrics::default();
    // When metrics are rendered before any admission.
    let text = metrics.render();
    // Then the bounded series explicitly reports zero.
    assert!(text.contains(&format!("{SERIES} 0\n")));
}

#[test]
fn concurrent_shared_limit_conflicts_accumulate_in_one_bounded_series() {
    // Given one metric registry shared across independent admission threads.
    let metrics = Metrics::default();
    // When each thread records its conflicts concurrently.
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..CONFLICTS {
                    metrics.credentials().record_shared_limit_conflict();
                }
            });
        }
    });
    // Then no updates are lost and the provider label is the complete label set.
    let text = metrics.render();
    let samples: Vec<_> = text
        .lines()
        .filter(|line| line.starts_with("llm3_shared_limit_conflicts_total{"))
        .collect();
    assert_eq!(samples, [format!("{SERIES} {}", THREADS * CONFLICTS)]);
}
