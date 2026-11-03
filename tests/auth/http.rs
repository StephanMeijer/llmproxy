use llmproxy::{
    config::{Config, PostgresConfig},
    proxy::{Proxy, router},
};
use std::sync::Arc;
use tokio::{net::TcpListener, task::JoinSet};

pub async fn start(postgres: &PostgresConfig) -> (String, JoinSet<()>) {
    let mut servers = JoinSet::new();
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    servers.spawn(async move {
        axum::serve(upstream, axum::Router::new().fallback(|| async { "ok" }))
            .await
            .unwrap();
    });
    let text = format!(
        "listen='127.0.0.1:0'\nproxy_token='legacy'\n[postgres]\nurl='{}'\nnamespace='test'\nacquire_timeout='1s'\nconnect_timeout='1s'\n[credential_encryption]\nactive_key_id='test'\nfingerprint_key_file='/missing'\nkeys={{test='/missing'}}\n[claude]\n[[route]]\nname='static'\nupstream='http://{upstream_addr}'\n",
        postgres.url
    );
    let proxy = Proxy::new(Config::parse(&text, |_| None).unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    servers.spawn(async move {
        axum::serve(listener, router(Arc::new(proxy)))
            .await
            .unwrap();
    });
    (base, servers)
}
