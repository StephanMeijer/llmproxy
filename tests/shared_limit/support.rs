use llmproxy::limit::shared::{AdmissionStore, SharedCharge, SharedRequest};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Default)]
pub struct State {
    pub definitions: HashMap<(String, String), serde_json::Value>,
    pub charged: HashMap<(Uuid, String), usize>,
    pub leases: usize,
    pub order: Vec<String>,
}

pub struct Replica {
    pub state: Arc<Mutex<State>>,
    pub fail: Option<String>,
}
pub struct Lease(Arc<Mutex<State>>);
impl Drop for Lease {
    fn drop(&mut self) {
        let state = self.0.lock();
        // Always release a healthy lease; skip only the poison panic while unwinding.
        if state.is_ok() || !std::thread::panicking() {
            state.unwrap().leases -= 1;
        }
    }
}
impl AdmissionStore for Replica {
    type Lease = Lease;
    type Error = ();
    async fn acquire(&self, charge: SharedCharge<'_>) -> Result<Lease, ()> {
        let mut state = self.state.lock().unwrap();
        let d = charge.definition;
        let key = (d.namespace().to_owned(), d.name().to_owned());
        if self.fail.as_deref() == Some(d.name()) {
            return Err(());
        }
        let value = serde_json::to_value(d).unwrap();
        match state.definitions.get(&key) {
            Some(stored) if *stored != value => return Err(()),
            Some(_) => {}
            None => {
                state.definitions.insert(key, value);
            }
        }
        *state
            .charged
            .entry((charge.person, d.name().to_owned()))
            .or_default() += charge.tokens;
        state.order.push(d.name().to_owned());
        state.leases += 1;
        drop(state);
        Ok(Lease(self.state.clone()))
    }
}

pub const fn request(person: Uuid) -> SharedRequest<'static> {
    SharedRequest {
        person,
        path: "/claude/v1/messages",
        model: None,
        body_bytes: 43,
    }
}
