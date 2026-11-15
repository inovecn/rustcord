//! Protocolo compartilhado entre servidor e cliente do Rustcord.
//!
//! Texto: uma mensagem por linha, campos separados por TAB, com escape de
//! `\\`, `\t`, `\n` e `\r` dentro dos campos.
//!
//! Voz: pacotes UDP.
//!   cliente -> servidor: [token: u64 BE][frame PCM]   (frame vazio = "hello"/keepalive)
//!   servidor -> cliente: [len_nome: u8][nome][frame PCM]
//! Frame PCM = 20 ms, 16 kHz, mono, s16le = 640 bytes.

pub const VOICE_FRAME_BYTES: usize = 640;
pub const VOICE_FRAME_SAMPLES: usize = VOICE_FRAME_BYTES / 2;
pub const DEFAULT_PORT: u16 = 7878;

pub fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '\t' => o.push_str("\\t"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            _ => o.push(c),
        }
    }
    o
}

pub fn unesc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('t') => o.push('\t'),
                Some('n') => o.push('\n'),
                Some('r') => o.push('\r'),
                Some('\\') => o.push('\\'),
                Some(x) => {
                    o.push('\\');
                    o.push(x);
                }
                None => o.push('\\'),
            }
        } else {
            o.push(c);
        }
    }
    o
}

pub fn encode(fields: &[&str]) -> String {
    fields.iter().map(|f| esc(f)).collect::<Vec<_>>().join("\t")
}

pub fn decode(line: &str) -> Vec<String> {
    line.trim_end_matches(['\r', '\n']).split('\t').map(unesc).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = ["MSG", "a\tb", "linha1\nlinha2", "barra\\invertida", ""];
        let enc = encode(&f);
        assert!(!enc.contains('\n'));
        let dec = decode(&enc);
        assert_eq!(dec, f.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    }
}
