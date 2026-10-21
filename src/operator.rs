use std::{env, error::Error, io::Write, num::NonZeroUsize, path::PathBuf};

use llmproxy::{
    config::Config,
    secret::{KeyRing, ProxyToken, TokenHasher},
    storage::{
        CredentialState, CredentialVersion, Credentials, Person, PersonId, Storage, TokenId,
        TokenRecord,
    },
};
use serde_json::{Value, json};
use uuid::Uuid;

mod input;

enum Output {
    Metadata(Value),
    Issued { person: Person, token: ProxyToken },
}

#[derive(serde::Serialize)]
struct IssuedToken<'a> {
    person_id: Uuid,
    label: &'a str,
    token_id: Uuid,
    token: &'a str,
}

enum Command<'a> {
    Create(&'a str),
    Person(PersonId, bool),
    Issue(PersonId),
    Revoke(TokenId),
    Claude(PersonId, Claude),
    Reencrypt(&'a str),
}

enum Claude {
    Set(bool),
    Remove,
    Status,
}

impl<'a> Command<'a> {
    fn parse(args: &'a [String]) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let words: Vec<&str> = args.iter().map(String::as_str).collect();
        Ok(match words.as_slice() {
            ["auth", "person", "create", name] if !name.trim().is_empty() => Self::Create(name),
            ["auth", "person", "status", id] => Self::Person(PersonId(Uuid::parse_str(id)?), false),
            ["auth", "person", "disable", id] => Self::Person(PersonId(Uuid::parse_str(id)?), true),
            ["auth", "token", "issue", id] => Self::Issue(PersonId(Uuid::parse_str(id)?)),
            ["auth", "token", "revoke", id] => Self::Revoke(TokenId(Uuid::parse_str(id)?)),
            ["auth", "claude", "set", id] => Self::Claude(PersonId(Uuid::parse_str(id)?), Claude::Set(false)),
            ["auth", "claude", "set", id, "--credential-stdin"] => Self::Claude(PersonId(Uuid::parse_str(id)?), Claude::Set(true)),
            ["auth", "claude", "remove", id] => Self::Claude(PersonId(Uuid::parse_str(id)?), Claude::Remove),
            ["auth", "claude", "status", id] => Self::Claude(PersonId(Uuid::parse_str(id)?), Claude::Status),
            ["auth", "credentials", "reencrypt", "--to", target] => Self::Reencrypt(target),
            _ => return Err("usage: llmproxy auth person create <label>|disable|status <person-id>; auth token issue <person-id>|revoke <token-id>; auth claude set|remove|status <person-id> [--credential-stdin]; auth credentials reencrypt --to <key-id> (config: LLMPROXY_CONFIG)".into()),
        })
    }
}

pub async fn run(args: &[String]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let command = Command::parse(args)?;
    let path = env::var(super::CONFIG_ENV).unwrap_or_else(|_| super::DEFAULT_CONFIG.to_owned());
    let config = Config::load(&PathBuf::from(path))?;
    let store = Storage::new(
        config
            .postgres
            .as_ref()
            .ok_or("operator commands require [postgres]")?,
    )?;
    let result = execute(command, &store, &config).await;
    store.close().await;
    let output = result?;
    let mut stdout = std::io::stdout().lock();
    match output {
        Output::Metadata(value) => serde_json::to_writer(&mut stdout, &value)?,
        Output::Issued { person, token } => serde_json::to_writer(
            &mut stdout,
            &IssuedToken {
                person_id: person.id.0,
                label: &person.display_name,
                token_id: token.id(),
                token: token.expose(),
            },
        )?,
    }
    writeln!(stdout)?;
    Ok(())
}

async fn execute(
    command: Command<'_>,
    store: &Storage,
    config: &Config,
) -> Result<Output, Box<dyn Error + Send + Sync>> {
    match command {
        Command::Create(name) => {
            let mut tx = store.begin().await?;
            let person = tx.create_person(PersonId(Uuid::new_v4()), name).await?;
            tx.commit().await?;
            Ok(Output::Metadata(person_output(&person)))
        }
        Command::Person(id, disable) => {
            let mut tx = store.begin().await?;
            let mut person = tx.person(id).await?;
            if disable {
                person.enabled = false;
                person = tx.update_person(&person).await?;
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok(Output::Metadata(person_output(&person)))
        }
        Command::Issue(id) => {
            let token = ProxyToken::generate()?;
            let secret_hash = TokenHasher::new(NonZeroUsize::MIN).hash(&token).await?;
            let mut tx = store.begin().await?;
            let person = tx.person(id).await?;
            if !person.enabled {
                return Err("person is disabled".into());
            }
            tx.create_token(&TokenRecord {
                id: TokenId(token.id()),
                person_id: id,
                secret_hash,
                revoked: false,
                person_enabled: true,
            })
            .await?;
            tx.commit().await?;
            Ok(Output::Issued { person, token })
        }
        Command::Revoke(id) => {
            let mut tx = store.begin().await?;
            let token = tx.token(id).await?;
            let person = tx.person(token.person_id).await?;
            tx.revoke_token(id).await?;
            tx.commit().await?;
            Ok(Output::Metadata(
                json!({"token_id": id.0, "person_id": person.id.0, "label": person.display_name, "revoked": true}),
            ))
        }
        Command::Claude(id, operation) => claude(id, operation, (store, config))
            .await
            .map(Output::Metadata),
        Command::Reencrypt(target) => {
            let encryption = config
                .credential_encryption
                .as_ref()
                .ok_or("credential encryption unavailable")?;
            let counts = store.reencrypt_credentials(encryption, target).await?;
            Ok(Output::Metadata(
                json!({"rewritten": counts.rewritten, "remaining": counts.remaining}),
            ))
        }
    }
}

fn person_output(person: &llmproxy::storage::Person) -> Value {
    json!({"person_id": person.id.0, "label": person.display_name, "enabled": person.enabled, "version": person.version, "created_at": person.created_at})
}

async fn claude(
    id: PersonId,
    operation: Claude,
    context: (&Storage, &Config),
) -> Result<Value, Box<dyn Error + Send + Sync>> {
    let (store, config) = context;
    let secret = match operation {
        Claude::Set(from_stdin) => Some(input::credential(from_stdin)?),
        Claude::Remove | Claude::Status => None,
    };
    let mut tx = store.begin().await?;
    let person = tx.person(id).await?;
    let record = tx.credential(id).await?;
    tx.rollback().await?;
    let version = record.as_ref().map(|record| CredentialVersion {
        person: id,
        generation: record.generation,
    });
    let (state, generation) = match operation {
        Claude::Status => match record {
            Some(record) => (
                match record.state {
                    CredentialState::Active => "active",
                    CredentialState::Invalid => "invalid",
                    CredentialState::Removed => "removed",
                },
                Some(record.generation),
            ),
            None => ("missing", None),
        },
        Claude::Set(_) => {
            if !person.enabled {
                return Err("person is disabled".into());
            }
            let encryption = config
                .credential_encryption
                .as_ref()
                .ok_or("credential encryption unavailable")?;
            let keys = KeyRing::load(&encryption.active_key_id, &encryption.keys)?;
            let fingerprint = input::fingerprint(&encryption.fingerprint_key_file)?;
            let credentials = Credentials::new(store.clone(), keys, fingerprint);
            let secret = secret.as_ref().ok_or("credential input unavailable")?;
            let version = match version {
                Some(version) => credentials.replace(version, secret).await?,
                None => credentials.install(id, secret).await?,
            };
            ("active", Some(version.generation))
        }
        Claude::Remove => {
            let mut record = record.ok_or("credential not found")?;
            record.state = CredentialState::Removed;
            let mut tx = store.begin().await?;
            let generation = tx.set_credential_state(&record).await?;
            tx.commit().await?;
            ("removed", Some(generation))
        }
    };
    Ok(
        json!({"person_id": id.0, "label": person.display_name, "state": state, "generation": generation}),
    )
}
