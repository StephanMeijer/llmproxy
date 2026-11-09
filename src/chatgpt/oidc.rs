//! OIDC helpers for `OpenAI` sign-in: PKCE, discovery, JWKS and RS256 ID-token checks.

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents};
use serde::{Deserialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[cfg(test)]
mod tests;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const KEY_REFETCH_INTERVAL: Duration = Duration::from_mins(1);
const EXPIRY_LEEWAY_SECONDS: u64 = 60;
const RANDOM_BYTES: usize = 32;

/// Fixed messages only: never the token, a claim or a fetched body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OidcError {
    Randomness,
    Fetch,
    TooLarge,
    IssuerMismatch,
    Malformed,
    Algorithm,
    UnknownKey,
    Signature,
    Issuer,
    Audience,
    Expired,
    Nonce,
    Subject,
}

impl fmt::Display for OidcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Randomness => "secure randomness unavailable",
            Self::Fetch => "OpenID provider request failed",
            Self::TooLarge => "OpenID provider document too large",
            Self::IssuerMismatch => "OpenID discovery names a different issuer",
            Self::Malformed => "malformed OpenID document or ID token",
            Self::Algorithm => "ID token algorithm is not RS256",
            Self::UnknownKey => "ID token signing key is unknown",
            Self::Signature => "ID token signature is invalid",
            Self::Issuer => "ID token issuer does not match",
            Self::Audience => "ID token audience does not match",
            Self::Expired => "ID token has expired",
            Self::Nonce => "ID token nonce does not match",
            Self::Subject => "ID token subject is empty",
        })
    }
}

impl std::error::Error for OidcError {}

/// PKCE S256 pair; the verifier is wiped on drop.
pub struct Pkce {
    pub verifier: Zeroizing<String>,
    pub challenge: String,
}

impl Pkce {
    /// # Errors
    /// Returns [`OidcError::Randomness`] when the OS cannot supply random bytes.
    pub fn new() -> Result<Self, OidcError> {
        let verifier = Zeroizing::new(random_token()?);
        let challenge = challenge(&verifier);
        Ok(Self {
            verifier,
            challenge,
        })
    }
}

/// `BASE64URL(SHA256(verifier))` without padding (RFC 7636 S256).
#[must_use]
pub fn challenge(verifier: &str) -> String {
    encode(&Sha256::digest(verifier.as_bytes()))
}

/// 32 random bytes as 43 base64url characters: `state` and `nonce`.
///
/// # Errors
/// Returns [`OidcError::Randomness`] when the OS cannot supply random bytes.
pub fn random_token() -> Result<String, OidcError> {
    let mut bytes = Zeroizing::new([0; RANDOM_BYTES]);
    getrandom::fill(&mut *bytes).map_err(|_| OidcError::Randomness)?;
    Ok(encode(&*bytes))
}

fn encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode(text: &str) -> Result<Vec<u8>, OidcError> {
    URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| OidcError::Malformed)
}

/// The parts of `/.well-known/openid-configuration` the sign-in flow uses.
#[derive(Debug, Clone, Deserialize)]
pub struct Discovery {
    pub issuer: String,
    pub jwks_uri: String,
    pub revocation_endpoint: String,
    pub token_endpoint: Option<String>,
    pub authorization_endpoint: Option<String>,
}

/// An RSA public key's big-endian modulus and exponent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicKey {
    pub n: Vec<u8>,
    pub e: Vec<u8>,
}

/// Usable signing keys by `kid`.
pub type Keys = HashMap<String, PublicKey>;

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kty: String,
    kid: Option<String>,
    n: Option<String>,
    e: Option<String>,
    alg: Option<String>,
    #[serde(rename = "use")]
    usage: Option<String>,
}

/// Fetches discovery for `issuer` and requires it to name exactly that issuer.
///
/// # Errors
/// [`OidcError::Fetch`], [`OidcError::TooLarge`], [`OidcError::Malformed`] or
/// [`OidcError::IssuerMismatch`].
pub async fn discover(client: &reqwest::Client, issuer: &str) -> Result<Discovery, OidcError> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let discovery: Discovery = fetch_json(client, &url).await?;
    if discovery.issuer != issuer {
        return Err(OidcError::IssuerMismatch);
    }
    Ok(discovery)
}

/// Fetches a JWKS and keeps the RSA signing keys that have a `kid`; others are ignored.
///
/// # Errors
/// [`OidcError::Fetch`], [`OidcError::TooLarge`] or [`OidcError::Malformed`].
pub async fn fetch_keys(client: &reqwest::Client, jwks_uri: &str) -> Result<Keys, OidcError> {
    let jwks: Jwks = fetch_json(client, jwks_uri).await?;
    Ok(jwks
        .keys
        .into_iter()
        .filter(|key| {
            key.kty == "RSA"
                && key.alg.as_deref().is_none_or(|alg| alg == "RS256")
                && key.usage.as_deref().is_none_or(|usage| usage == "sig")
        })
        .filter_map(|key| {
            let n = decode(key.n.as_deref()?).ok()?;
            let e = decode(key.e.as_deref()?).ok()?;
            Some((key.kid?, PublicKey { n, e }))
        })
        .collect())
}

async fn fetch_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T, OidcError> {
    let mut response = client
        .get(url)
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|_| OidcError::Fetch)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| OidcError::Fetch)? {
        if body.len() + chunk.len() > MAX_DOCUMENT_BYTES {
            return Err(OidcError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| OidcError::Malformed)
}

/// What a verified ID token says about the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdClaims {
    pub sub: String,
    pub email: Option<String>,
    pub exp: u64,
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    kid: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    aud: Audience,
    azp: Option<String>,
    exp: u64,
    nonce: Option<String>,
    sub: String,
    email: Option<String>,
}

/// Verifies an RS256 ID token's signature against `keys`, then its `iss`, `aud` and `azp`, `exp` (with up
/// to 60 s leeway against `now`, Unix seconds), `nonce` and a non-empty `sub`.
///
/// # Errors
/// An [`OidcError`] naming the first check that failed; every other `alg` is refused.
pub fn verify_id_token(
    jwt: &str,
    keys: &Keys,
    expected_issuer: &str,
    expected_audience: &str,
    expected_nonce: &str,
    now: u64,
) -> Result<IdClaims, OidcError> {
    let mut parts = jwt.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(OidcError::Malformed);
    };
    let parsed: Header =
        serde_json::from_slice(&decode(header)?).map_err(|_| OidcError::Malformed)?;
    if parsed.alg != "RS256" {
        return Err(OidcError::Algorithm);
    }
    let key = parsed
        .kid
        .and_then(|kid| keys.get(&kid))
        .ok_or(OidcError::UnknownKey)?;
    RsaPublicKeyComponents {
        n: &key.n,
        e: &key.e,
    }
    .verify(
        &RSA_PKCS1_2048_8192_SHA256,
        format!("{header}.{payload}").as_bytes(),
        &decode(signature)?,
    )
    .map_err(|_| OidcError::Signature)?;
    let claims: Claims =
        serde_json::from_slice(&decode(payload)?).map_err(|_| OidcError::Malformed)?;
    if claims.iss != expected_issuer {
        return Err(OidcError::Issuer);
    }
    let (listed, azp_required) = match &claims.aud {
        Audience::One(aud) => (aud == expected_audience, false),
        Audience::Many(auds) => (
            auds.iter().any(|aud| aud == expected_audience),
            auds.len() > 1,
        ),
    };
    // OIDC Core 3.1.3.7: several audiences need `azp`, and a present `azp` must be our client.
    let azp_ok = claims
        .azp
        .as_deref()
        .map_or(!azp_required, |azp| azp == expected_audience);
    if !listed || !azp_ok {
        return Err(OidcError::Audience);
    }
    if claims.exp.saturating_add(EXPIRY_LEEWAY_SECONDS) <= now {
        return Err(OidcError::Expired);
    }
    if claims.nonce.as_deref() != Some(expected_nonce) {
        return Err(OidcError::Nonce);
    }
    if claims.sub.is_empty() {
        return Err(OidcError::Subject);
    }
    Ok(IdClaims {
        sub: claims.sub,
        email: claims.email,
        exp: claims.exp,
    })
}

/// Discovery and signing keys for one issuer; keys are refetched on an unknown `kid`, at most
/// once a minute.
// shortcut: discovery is fetched once per Cache, upgrade to a TTL if OpenAI moves endpoints.
pub struct Cache {
    discovery: Discovery,
    keys: Keys,
    keys_fetched_at: Instant,
}

impl Cache {
    /// # Errors
    /// Any error of [`discover`] or [`fetch_keys`].
    pub async fn load(client: &reqwest::Client, issuer: &str) -> Result<Self, OidcError> {
        let discovery = discover(client, issuer).await?;
        let keys = fetch_keys(client, &discovery.jwks_uri).await?;
        Ok(Self {
            discovery,
            keys,
            keys_fetched_at: Instant::now(),
        })
    }

    #[must_use]
    pub const fn discovery(&self) -> &Discovery {
        &self.discovery
    }

    /// [`verify_id_token`] against the discovered issuer, refetching keys once on an unknown `kid`.
    ///
    /// # Errors
    /// Any error of [`verify_id_token`] or [`fetch_keys`].
    pub async fn verify(
        &mut self,
        client: &reqwest::Client,
        jwt: &str,
        audience: &str,
        nonce: &str,
        now: u64,
    ) -> Result<IdClaims, OidcError> {
        let verify =
            |keys: &Keys| verify_id_token(jwt, keys, &self.discovery.issuer, audience, nonce, now);
        match verify(&self.keys) {
            Err(OidcError::UnknownKey)
                if self.keys_fetched_at.elapsed() >= KEY_REFETCH_INTERVAL =>
            {
                // Counted before fetching so a failing JWKS endpoint is not hammered either.
                self.keys_fetched_at = Instant::now();
                self.keys = fetch_keys(client, &self.discovery.jwks_uri).await?;
                verify(&self.keys)
            }
            result => result,
        }
    }
}
