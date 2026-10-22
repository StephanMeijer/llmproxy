use llmproxy::secret::SecretBytes;
use std::io::{self, BufRead, IsTerminal, Read};
use zeroize::Zeroizing;

const MAX_CREDENTIAL_BYTES: usize = 4096;
const KEY_BYTES: usize = 32;

pub(super) fn fingerprint(
    path: &std::path::Path,
) -> Result<SecretBytes, Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::open(path).map_err(|_| "fingerprint key unavailable")?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(u64::try_from(KEY_BYTES + 1)?)
        .read_to_end(&mut bytes)
        .map_err(|_| "fingerprint key unavailable")?;
    if bytes.len() != KEY_BYTES {
        return Err("fingerprint key must contain exactly 32 bytes".into());
    }
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}

pub(super) fn credential(
    from_stdin: bool,
) -> Result<SecretBytes, Box<dyn std::error::Error + Send + Sync>> {
    let mut bytes = if from_stdin {
        let mut bytes = Zeroizing::new(Vec::new());
        io::stdin()
            .lock()
            .take(u64::try_from(MAX_CREDENTIAL_BYTES + 2)?)
            .read_until(b'\n', &mut bytes)
            .map_err(|_| "credential input unavailable")?;
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
        }
        bytes
    } else {
        if !io::stdin().is_terminal() {
            return Err("credential input requires a terminal or --credential-stdin".into());
        }
        Zeroizing::new(
            rpassword::prompt_password("Anthropic API key: ")
                .map_err(|_| "credential input unavailable")?
                .into_bytes(),
        )
    };
    if bytes.is_empty()
        || bytes.len() > MAX_CREDENTIAL_BYTES
        || !bytes.iter().all(u8::is_ascii_graphic)
    {
        return Err("credential must contain 1–4096 visible ASCII bytes".into());
    }
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}
