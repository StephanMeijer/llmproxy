use super::database;
use llmproxy::{
    config::Config,
    limit::shared::{CompiledLimits, SharedCharge, SharedDefinition, SharedRateStore},
    storage::{self, Lease, LimitKey, OwnerId, PersonId, ReservationId, Storage},
};
use std::time::Duration;
use uuid::Uuid;

pub const SHORT: Duration = Duration::from_millis(350);
pub const LONG: Duration = Duration::from_secs(5);

pub struct Fixture {
    pub db: database::Database,
    pub first: Storage,
    pub second: Storage,
    pub alice: PersonId,
    pub bob: PersonId,
}
impl Fixture {
    pub async fn new() -> Self {
        let db = database::Database::new().await;
        storage::migrate(&db.config).await.unwrap();
        let first = Storage::new(&db.config).unwrap();
        let second = Storage::new(&db.config).unwrap();
        let alice = PersonId(Uuid::new_v4());
        let bob = PersonId(Uuid::new_v4());
        let mut tx = first.begin().await.unwrap();
        tx.create_person(alice, "Alice").await.unwrap();
        tx.create_person(bob, "Bob").await.unwrap();
        tx.commit().await.unwrap();
        Self {
            db,
            first,
            second,
            alice,
            bob,
        }
    }
    pub fn replica(&self, second: bool, max_wait: Duration) -> SharedRateStore {
        SharedRateStore::new(
            if second {
                self.second.clone()
            } else {
                self.first.clone()
            },
            max_wait,
        )
    }
    pub async fn close(self) {
        self.first.close().await;
        self.second.close().await;
        self.db.close().await;
    }
    pub async fn waiters(&self, count: i64) {
        tokio::time::timeout(LONG, async {
            loop {
                let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM fifo_waiters")
                    .fetch_one(&self.db.pool)
                    .await
                    .unwrap();
                if actual == count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    pub async fn refill(&self) {
        sqlx::query("UPDATE rate_state SET updated_at=clock_timestamp()-interval '61 seconds'")
            .execute(&self.db.pool)
            .await
            .unwrap();
    }
}
pub fn definitions(body: &str) -> Vec<SharedDefinition> {
    let config = Config::parse(
        &format!("listen='127.0.0.1:0'\nproxy_token='t'\n{body}"),
        |_| None,
    )
    .unwrap();
    CompiledLimits::new(&config.limits, "test").unwrap().shared
}
pub fn definition(body: &str) -> SharedDefinition {
    definitions(&format!(
        "[[limit]]\nname='budget'\nprefixes=['/claude/']\n{body}"
    ))
    .remove(0)
}
pub const fn charge(
    definition: &SharedDefinition,
    person: PersonId,
    tokens: usize,
) -> SharedCharge<'_> {
    SharedCharge {
        definition,
        person: person.0,
        tokens,
    }
}
pub fn key(person: PersonId) -> LimitKey {
    LimitKey {
        person,
        name: "budget".into(),
    }
}
pub fn reservation() -> Lease {
    Lease {
        id: ReservationId(Uuid::new_v4()),
        owner: OwnerId(Uuid::new_v4()),
    }
}
