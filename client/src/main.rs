//! Cliente desktop (terminal) do Rustcord. Apenas biblioteca padrão.

mod voice;

use rustcord_proto::{decode, encode, DEFAULT_PORT};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Clone)]
struct Chan {
    id: u64,
    kind: String,
    name: String,
}

#[derive(Default)]
struct Cs {
    me: String,
    servers: Vec<(u64, String, String)>, // id, nome, convite
    channels: HashMap<u64, Vec<Chan>>,   // servidor -> canais
    cur_server: Option<u64>,
    cur_chan: Option<u64>,
    cur_dm: Option<String>,
    voice_occ: HashMap<u64, Vec<String>>, // canal de voz -> usuários
    voice: Option<voice::Voice>,
    host: String,
}

type Shared = Arc<Mutex<Cs>>;
type Writer = Arc<Mutex<TcpStream>>;

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const CYAN: &str = "\x1b[36m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

fn send(w: &Writer, fields: &[&str]) {
    let mut s = w.lock().unwrap();
    let _ = writeln!(s, "{}", encode(fields));
    let _ = s.flush();
}

fn hhmm(ts: u64) -> String {
    let s = ts % 86400;
    format!("{:02}:{:02}", s / 3600, (s % 3600) / 60)
}

fn chan_name(cs: &Cs, cid: u64) -> String {
    for list in cs.channels.values() {
        if let Some(c) = list.iter().find(|c| c.id == cid) {
            return c.name.clone();
        }
    }
    format!("canal-{cid}")
}

fn find_chan(cs: &Cs, key: &str, kind: Option<&str>) -> Option<Chan> {
    let sid = cs.cur_server?;
    let key = key.trim_start_matches('#');
    let list = cs.channels.get(&sid)?;
    list.iter()
        .filter(|c| kind.map(|k| k == c.kind).unwrap_or(true))
        .find(|c| c.id.to_string() == key || c.name.eq_ignore_ascii_case(key))
        .cloned()
}

fn handle_event(f: Vec<String>, shared: &Shared, w: &Writer) {
    let g = |i: usize| f.get(i).map(|s| s.as_str()).unwrap_or("");
    let num = |i: usize| g(i).parse::<u64>().unwrap_or(0);
    let mut cs = shared.lock().unwrap();
    match g(0) {
        "HELLO" | "END" | "PONG" => {}
        "ERR" => println!("{RED}✖ {}{RESET}", g(1)),
        "AUTH_OK" => {
            cs.me = g(1).to_string();
            println!("{GREEN}✔ Logado como {BOLD}{}{RESET}{GREEN}. Digite /help para ver os comandos.{RESET}", g(1));
            drop(cs);
            send(w, &["SERVERS"]);
        }
        "SERVER" => {
            let (id, name, invite) = (num(1), g(2).to_string(), g(3).to_string());
            if let Some(s) = cs.servers.iter_mut().find(|s| s.0 == id) {
                s.1 = name.clone();
                s.2 = invite.clone();
            } else {
                cs.servers.push((id, name.clone(), invite.clone()));
            }
            if cs.cur_server.is_none() {
                cs.cur_server = Some(id);
            }
            println!("{CYAN}■ Servidor [{id}] {BOLD}{name}{RESET}{CYAN}  (convite: {invite}, dono: {}){RESET}", g(4));
            drop(cs);
            send(w, &["CHANNELS", &id.to_string()]);
        }
        "CHANNEL" => {
            let (sid, id) = (num(1), num(2));
            let ch = Chan { id, kind: g(3).to_string(), name: g(4).to_string() };
            let list = cs.channels.entry(sid).or_default();
            if let Some(c) = list.iter_mut().find(|c| c.id == id) {
                *c = ch.clone();
            } else {
                list.push(ch.clone());
            }
            if cs.cur_chan.is_none() && ch.kind == "text" && cs.cur_server == Some(sid) {
                cs.cur_chan = Some(id);
            }
            let icon = if ch.kind == "voice" { "🔊" } else { "#" };
            println!("{DIM}  {icon} {} (id {}){RESET}", ch.name, ch.id);
        }
        "MEMBER" => {
            let dot = if g(3) == "1" { format!("{GREEN}●{RESET}") } else { format!("{DIM}○{RESET}") };
            println!("  {dot} {}", g(2));
        }
        "MEMBER_JOIN" => println!("{YELLOW}→ {} entrou no servidor {}{RESET}", g(2), g(1)),
        "PRESENCE" => println!("{DIM}{} ficou {}{RESET}", g(1), if g(2) == "on" { "online" } else { "offline" }),
        "MSG" | "HIST" => {
            let cid = num(1);
            let name = chan_name(&cs, cid);
            println!("{DIM}{} #{}{RESET} {BOLD}{}{RESET}: {}", hhmm(num(2)), name, g(3), g(4));
        }
        "DM" | "DMHIST" => {
            let (from, to) = (g(2), g(3));
            let other = if from == cs.me { to } else { from };
            println!("{DIM}{} DM{RESET} {BOLD}{}{RESET} → {}: {}", hhmm(num(1)), from, other.to_string().replace(&cs.me, "você"), g(4));
        }
        "VOICE_STATE" => {
            let cid = num(1);
            let user = g(2).to_string();
            let occ = cs.voice_occ.entry(cid).or_default();
            occ.retain(|u| *u != user);
            let joined = g(3) == "joined";
            if joined {
                occ.push(user.clone());
            }
            let name = chan_name(&cs, cid);
            println!("{YELLOW}🔊 {} {} o canal de voz {}{RESET}", user, if joined { "entrou em" } else { "saiu de" }, name);
        }
        "VOICE_OK" => {
            let (cid, port, token) = (num(1), g(2).parse::<u16>().unwrap_or(0), num(3));
            if let Some(old) = cs.voice.take() {
                old.stop();
            }
            let host = cs.host.clone();
            let addr = (host.as_str(), port).to_socket_addrs().ok().and_then(|mut a| a.next());
            match addr {
                None => println!("{RED}✖ Não consegui resolver o endereço de voz.{RESET}"),
                Some(addr) => match voice::start(addr, token, cid, |m| println!("{YELLOW}! {m}{RESET}")) {
                    Ok(v) => {
                        println!("{GREEN}🎙 Conectado à voz ({}). /mute silencia, /leave sai.{RESET}", chan_name(&cs, cid));
                        cs.voice = Some(v);
                    }
                    Err(e) => println!("{RED}✖ Falha ao iniciar voz: {e}{RESET}"),
                },
            }
        }
        other => println!("{DIM}(evento desconhecido: {other}){RESET}"),
    }
}

fn help() {
    println!(
        "{BOLD}Comandos{RESET}
  /register <usuario> <senha>     cria conta
  /login <usuario> <senha>        entra
  /servers                        lista seus servidores
  /create <nome>                  cria servidor
  /join <convite>                 entra num servidor
  /use <id|nome>                  escolhe o servidor atual
  /channels                       lista canais do servidor atual
  /newchannel text|voice <nome>   cria canal (dono)
  /go <canal>                     entra num canal de texto e mostra o histórico
  /members                        membros e quem está online
  /dm <usuario> [mensagem]        abre DM (ou envia mensagem direta)
  /voice <canal>                  entra num canal de voz
  /mute                           liga/desliga o microfone
  /leave                          sai da voz
  /quit                           sai
  (texto normal)                  envia para o canal ou DM atual"
    );
}

fn command(line: &str, shared: &Shared, w: &Writer) -> bool {
    let mut parts = line.splitn(3, ' ');
    let cmd = parts.next().unwrap_or("");
    let a = parts.next().unwrap_or("").trim();
    let b = parts.next().unwrap_or("").trim();
    match cmd {
        "/help" => help(),
        "/quit" | "/exit" => {
            send(w, &["QUIT"]);
            return false;
        }
        "/register" => send(w, &["REGISTER", a, b]),
        "/login" => send(w, &["LOGIN", a, b]),
        "/servers" => send(w, &["SERVERS"]),
        "/create" => {
            let name = line.strip_prefix("/create").unwrap_or("").trim();
            send(w, &["SERVER_CREATE", name]);
        }
        "/join" => send(w, &["SERVER_JOIN", a]),
        "/use" => {
            let key = line.strip_prefix("/use").unwrap_or("").trim();
            let mut cs = shared.lock().unwrap();
            let found = cs
                .servers
                .iter()
                .find(|s| s.0.to_string() == key || s.1.eq_ignore_ascii_case(key))
                .map(|s| (s.0, s.1.clone()));
            match found {
                Some((id, name)) => {
                    cs.cur_server = Some(id);
                    cs.cur_chan = None;
                    cs.cur_dm = None;
                    println!("{CYAN}Servidor atual: {name}{RESET}");
                    drop(cs);
                    send(w, &["CHANNELS", &id.to_string()]);
                }
                None => println!("{RED}✖ Servidor não encontrado. Veja /servers.{RESET}"),
            }
        }
        "/channels" => match shared.lock().unwrap().cur_server {
            Some(id) => send(w, &["CHANNELS", &id.to_string()]),
            None => println!("{RED}✖ Escolha um servidor com /use.{RESET}"),
        },
        "/members" => match shared.lock().unwrap().cur_server {
            Some(id) => send(w, &["MEMBERS", &id.to_string()]),
            None => println!("{RED}✖ Escolha um servidor com /use.{RESET}"),
        },
        "/newchannel" => match shared.lock().unwrap().cur_server {
            Some(id) => send(w, &["CHANNEL_CREATE", &id.to_string(), a, b]),
            None => println!("{RED}✖ Escolha um servidor com /use.{RESET}"),
        },
        "/go" => {
            let mut cs = shared.lock().unwrap();
            match find_chan(&cs, a, Some("text")) {
                Some(c) => {
                    cs.cur_chan = Some(c.id);
                    cs.cur_dm = None;
                    println!("{CYAN}— #{} —{RESET}", c.name);
                    drop(cs);
                    send(w, &["HISTORY", &c.id.to_string(), "30"]);
                }
                None => println!("{RED}✖ Canal de texto não encontrado. Veja /channels.{RESET}"),
            }
        }
        "/dm" => {
            let text = b;
            shared.lock().unwrap().cur_dm = Some(a.to_string());
            if text.is_empty() {
                println!("{CYAN}— DM com {a} —{RESET}");
                send(w, &["DM_HISTORY", a, "30"]);
            } else {
                send(w, &["DM", a, text]);
            }
        }
        "/voice" => {
            let key = line.strip_prefix("/voice").unwrap_or("").trim();
            let cs = shared.lock().unwrap();
            match find_chan(&cs, key, Some("voice")) {
                Some(c) => send(w, &["VOICE_JOIN", &c.id.to_string()]),
                None => println!("{RED}✖ Canal de voz não encontrado. Veja /channels.{RESET}"),
            }
        }
        "/mute" => {
            let cs = shared.lock().unwrap();
            match &cs.voice {
                Some(v) => {
                    let now = !v.muted.load(std::sync::atomic::Ordering::SeqCst);
                    v.muted.store(now, std::sync::atomic::Ordering::SeqCst);
                    println!("{YELLOW}🎙 Microfone {}{RESET}", if now { "mutado" } else { "ligado" });
                }
                None => println!("{RED}✖ Você não está em um canal de voz.{RESET}"),
            }
        }
        "/leave" => {
            let mut cs = shared.lock().unwrap();
            if let Some(v) = cs.voice.take() {
                v.stop();
                send(w, &["VOICE_LEAVE"]);
                println!("{YELLOW}Você saiu da voz.{RESET}");
            }
        }
        _ => println!("{RED}✖ Comando desconhecido. /help lista os comandos.{RESET}"),
    }
    true
}

fn main() {
    let arg = std::env::args().nth(1).unwrap_or_else(|| format!("127.0.0.1:{DEFAULT_PORT}"));
    let addr = if arg.contains(':') { arg } else { format!("{arg}:{DEFAULT_PORT}") };
    let host = addr.rsplit_once(':').map(|x| x.0.to_string()).unwrap_or_default();

    let stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Não consegui conectar em {addr}: {e}");
            std::process::exit(1);
        }
    };
    let _ = stream.set_nodelay(true);
    let reader = BufReader::new(stream.try_clone().expect("clone"));
    let writer: Writer = Arc::new(Mutex::new(stream));
    let shared: Shared = Arc::new(Mutex::new(Cs { host, ..Default::default() }));

    println!("{BOLD}Rustcord{RESET} conectado a {addr}. Use /register ou /login (e /help).");

    {
        let (shared, writer) = (shared.clone(), writer.clone());
        thread::spawn(move || {
            for line in reader.lines().map_while(Result::ok) {
                if line.is_empty() {
                    continue;
                }
                handle_event(decode(&line), &shared, &writer);
            }
            println!("{RED}Conexão com o servidor encerrada.{RESET}");
            std::process::exit(0);
        });
    }

    let stdin = std::io::stdin();
    for line in stdin.lock().lines().map_while(Result::ok) {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('/') {
            if !command(&line, &shared, &writer) {
                break;
            }
            continue;
        }
        let (chan, dm) = {
            let cs = shared.lock().unwrap();
            (cs.cur_chan, cs.cur_dm.clone())
        };
        // última conversa escolhida vence: DM se /dm foi usado depois do /go
        match (dm, chan) {
            (Some(user), _) => send(&writer, &["DM", &user, &line]),
            (None, Some(c)) => send(&writer, &["SEND", &c.to_string(), &line]),
            _ => println!("{RED}✖ Escolha um canal (/go) ou DM (/dm) primeiro.{RESET}"),
        }
    }
    let v = shared.lock().unwrap().voice.take();
    if let Some(v) = v {
        v.stop();
    }
}
