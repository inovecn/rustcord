//! Cliente de voz: captura (processo externo) -> UDP -> servidor -> mixagem -> reprodução (processo externo).
//!
//! Sem crates, o acesso ao microfone/alto-falante é feito por processos que leem/escrevem PCM cru
//! (s16le, 16 kHz, mono). No Linux o padrão é `arecord`/`aplay`. Em outros sistemas defina:
//!   RUSTCORD_CAPTURE_CMD   (escreve PCM no stdout)
//!   RUSTCORD_PLAYBACK_CMD  (lê PCM do stdin)

use rustcord_proto::{VOICE_FRAME_BYTES, VOICE_FRAME_SAMPLES};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct Voice {
    #[allow(dead_code)]
    pub chan: u64,
    stop: Arc<AtomicBool>,
    pub muted: Arc<AtomicBool>,
    children: Vec<Child>,
}

impl Voice {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

pub fn split_cmd(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut in_q = false;
    let mut has = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_q = !in_q;
                has = true;
            }
            c if c.is_whitespace() && !in_q => {
                if has || !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                    has = false;
                }
            }
            c => cur.push(c),
        }
    }
    if has || !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn default_cmd(env: &str, linux_default: &str) -> Option<String> {
    if let Ok(v) = std::env::var(env) {
        if !v.trim().is_empty() {
            return Some(v);
        }
    }
    if cfg!(target_os = "linux") {
        Some(linux_default.to_string())
    } else {
        None
    }
}

fn spawn(cmdline: &str, stdin: bool, stdout: bool) -> std::io::Result<Child> {
    let parts = split_cmd(cmdline);
    let (prog, args) = parts
        .split_first()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "comando vazio"))?;
    Command::new(prog)
        .args(args)
        .stdin(if stdin { Stdio::piped() } else { Stdio::null() })
        .stdout(if stdout { Stdio::piped() } else { Stdio::null() })
        .stderr(Stdio::null())
        .spawn()
}

pub fn start(server_udp: SocketAddr, token: u64, chan: u64, log: impl Fn(String) + Send + Clone + 'static) -> std::io::Result<Voice> {
    let sock = UdpSocket::bind("0.0.0.0:0")?;
    sock.connect(server_udp)?;
    sock.set_read_timeout(Some(Duration::from_millis(200)))?;
    let sock = Arc::new(sock);
    let stop = Arc::new(AtomicBool::new(false));
    let muted = Arc::new(AtomicBool::new(false));
    let mut children = vec![];
    let tok = token.to_be_bytes();

    let _ = sock.send(&tok); // hello: registra nosso endereço UDP no servidor

    // keepalive (mantém o mapeamento de NAT mesmo mutado/sem microfone)
    {
        let (sock, stop) = (sock.clone(), stop.clone());
        thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let _ = sock.send(&tok);
                for _ in 0..20 {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
    }

    // captura
    match default_cmd("RUSTCORD_CAPTURE_CMD", "arecord -q -t raw -f S16_LE -r 16000 -c 1") {
        None => log("Sem comando de captura (defina RUSTCORD_CAPTURE_CMD). Você só vai ouvir.".into()),
        Some(cmd) => match spawn(&cmd, false, true) {
            Err(e) => log(format!("Não consegui abrir o microfone ('{cmd}'): {e}. Você só vai ouvir.")),
            Ok(mut child) => {
                let mut out = child.stdout.take().unwrap();
                children.push(child);
                let (sock, stop, muted) = (sock.clone(), stop.clone(), muted.clone());
                thread::spawn(move || {
                    let mut frame = vec![0u8; VOICE_FRAME_BYTES];
                    let mut pkt = Vec::with_capacity(8 + VOICE_FRAME_BYTES);
                    while !stop.load(Ordering::SeqCst) {
                        if out.read_exact(&mut frame).is_err() {
                            break;
                        }
                        if muted.load(Ordering::SeqCst) {
                            continue;
                        }
                        pkt.clear();
                        pkt.extend_from_slice(&tok);
                        pkt.extend_from_slice(&frame);
                        let _ = sock.send(&pkt);
                    }
                });
            }
        },
    }

    // recepção -> filas por falante
    let queues: Arc<Mutex<HashMap<String, VecDeque<Vec<i16>>>>> = Arc::new(Mutex::new(HashMap::new()));
    {
        let (sock, stop, queues) = (sock.clone(), stop.clone(), queues.clone());
        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while !stop.load(Ordering::SeqCst) {
                let n = match sock.recv(&mut buf) {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if n < 2 {
                    continue;
                }
                let nl = buf[0] as usize;
                if n < 1 + nl + 2 {
                    continue;
                }
                let name = String::from_utf8_lossy(&buf[1..1 + nl]).to_string();
                let samples: Vec<i16> = buf[1 + nl..n]
                    .chunks_exact(2)
                    .take(VOICE_FRAME_SAMPLES)
                    .map(|c| i16::from_le_bytes([c[0], c[1]]))
                    .collect();
                let mut q = queues.lock().unwrap();
                let dq = q.entry(name).or_default();
                dq.push_back(samples);
                while dq.len() > 8 {
                    dq.pop_front(); // limita a latência
                }
            }
        });
    }

    // reprodução: mixa um frame de cada falante a cada 20 ms
    match default_cmd("RUSTCORD_PLAYBACK_CMD", "aplay -q -t raw -f S16_LE -r 16000 -c 1") {
        None => log("Sem comando de reprodução (defina RUSTCORD_PLAYBACK_CMD). Você só vai falar.".into()),
        Some(cmd) => match spawn(&cmd, true, false) {
            Err(e) => log(format!("Não consegui abrir o alto-falante ('{cmd}'): {e}. Você só vai falar.")),
            Ok(mut child) => {
                let mut input = child.stdin.take().unwrap();
                children.push(child);
                let (stop, queues) = (stop.clone(), queues.clone());
                thread::spawn(move || {
                    let tick = Duration::from_millis(20);
                    let mut next = Instant::now();
                    let mut out = vec![0u8; VOICE_FRAME_BYTES];
                    while !stop.load(Ordering::SeqCst) {
                        let mut mix = [0i32; VOICE_FRAME_SAMPLES];
                        {
                            let mut q = queues.lock().unwrap();
                            for dq in q.values_mut() {
                                if let Some(frame) = dq.pop_front() {
                                    for (i, s) in frame.iter().enumerate() {
                                        mix[i] += *s as i32;
                                    }
                                }
                            }
                        }
                        for (i, m) in mix.iter().enumerate() {
                            let s = (*m).clamp(i16::MIN as i32, i16::MAX as i32) as i16;
                            out[i * 2..i * 2 + 2].copy_from_slice(&s.to_le_bytes());
                        }
                        if input.write_all(&out).is_err() {
                            break;
                        }
                        next += tick;
                        let now = Instant::now();
                        if next > now {
                            thread::sleep(next - now);
                        } else {
                            next = now;
                        }
                    }
                });
            }
        },
    }

    Ok(Voice { chan, stop, muted, children })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_cmd_quotes() {
        assert_eq!(
            split_cmd(r#"ffmpeg -f dshow -i audio="Microfone (USB)" -"#),
            vec!["ffmpeg", "-f", "dshow", "-i", "audio=Microfone (USB)", "-"]
        );
        assert_eq!(split_cmd("  a   b "), vec!["a", "b"]);
    }
}
