use llmproxy::limit::shared::{AdmissionStore, SharedCharge, SharedRequest};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Default)]
pub struct State {
    pub definitions: HashMap<(String, String), [u8; 32]>,
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
        self.0.lock().unwrap().leases -= 1;
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
        match state.definitions.get(&key) {
            Some(hash) => d.verify(hash).map_err(|_| ())?,
            None => {
                state.definitions.insert(key, d.fingerprint().unwrap());
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
