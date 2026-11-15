//! Servidor do Rustcord: contas, servidores, canais de texto/voz, DMs e relay de voz UDP.
//! Apenas biblioteca padrão. Persistência por log append-only de eventos.

mod crypto;

use rustcord_proto::{decode, encode};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const PBKDF2_ITERS: u32 = 20_000;
const MAX_MSG_LEN: usize = 2000;
const MAX_LINE: usize = 16 * 1024;

struct UserRec {
    salt: Vec<u8>,
    hash: Vec<u8>,
}

struct Srv {
    name: String,
    owner: String,
    invite: String,
    members: Vec<String>,
    channels: Vec<u64>,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Text,
    Voice,
}

impl Kind {
    fn as_str(&self) -> &'static str {
        match self {
            Kind::Text => "text",
            Kind::Voice => "voice",
        }
    }
    fn parse(s: &str) -> Option<Kind> {
        match s {
            "text" => Some(Kind::Text),
            "voice" => Some(Kind::Voice),
            _ => None,
        }
    }
}

struct Chan {
    server: u64,
    kind: Kind,
    name: String,
}

#[derive(Clone)]
struct Msg {
    ts: u64,
    user: String,
    text: String,
}

struct Conn {
    user: Option<String>,
    tx: Sender<String>,
}

struct VoiceTok {
    user: String,
    chan: u64,
    addr: Option<SocketAddr>,
}

struct State {
    users: HashMap<String, UserRec>,
    servers: BTreeMap<u64, Srv>,
    channels: BTreeMap<u64, Chan>,
    msgs: HashMap<u64, Vec<Msg>>,
    dms: HashMap<(String, String), Vec<Msg>>,
    next_id: u64,
    conns: HashMap<u64, Conn>,
    voice_members: HashMap<u64, Vec<String>>,
    voice_tokens: HashMap<u64, VoiceTok>,
    log: Option<File>,
    udp_port: u16,
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn dm_key(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn valid_username(u: &str) -> bool {
    let n = u.chars().count();
    (2..=20).contains(&n) && u.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn clean_name(s: &str, max: usize) -> String {
    s.trim().chars().filter(|c| !c.is_control()).take(max).collect()
}

impl State {
    fn new() -> State {
        State {
            users: HashMap::new(),
            servers: BTreeMap::new(),
            channels: BTreeMap::new(),
            msgs: HashMap::new(),
            dms: HashMap::new(),
            next_id: 1,
            conns: HashMap::new(),
            voice_members: HashMap::new(),
            voice_tokens: HashMap::new(),
            log: None,
            udp_port: 0,
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn persist(&mut self, fields: &[&str]) {
        if let Some(f) = &mut self.log {
            let _ = writeln!(f, "{}", encode(fields));
        }
    }

    fn load(&mut self, path: &PathBuf) -> std::io::Result<()> {
        if let Ok(f) = File::open(path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                self.replay(&decode(&line));
            }
        }
        self.log = Some(OpenOptions::new().create(true).append(true).open(path)?);
        Ok(())
    }

    fn replay(&mut self, f: &[String]) {
        let g = |i: usize| f.get(i).map(|s| s.as_str()).unwrap_or("");
        let num = |i: usize| g(i).parse::<u64>().unwrap_or(0);
        match g(0) {
            "U" => {
                if let (Some(salt), Some(hash)) = (crypto::unhex(g(2)), crypto::unhex(g(3))) {
                    self.users.insert(g(1).to_string(), UserRec { salt, hash });
                }
            }
            "S" => {
                let id = num(1);
                self.servers.insert(
                    id,
                    Srv {
                        name: g(2).into(),
                        owner: g(3).into(),
                        invite: g(4).into(),
                        members: vec![],
                        channels: vec![],
                    },
                );
                self.next_id = self.next_id.max(id + 1);
            }
            "M" => {
                if let Some(s) = self.servers.get_mut(&num(1)) {
                    if !s.members.iter().any(|m| m == g(2)) {
                        s.members.push(g(2).into());
                    }
                }
            }
            "C" => {
                let (cid, sid) = (num(1), num(2));
                if let Some(kind) = Kind::parse(g(3)) {
                    self.channels.insert(cid, Chan { server: sid, kind, name: g(4).into() });
                    if let Some(s) = self.servers.get_mut(&sid) {
                        s.channels.push(cid);
                    }
                    self.next_id = self.next_id.max(cid + 1);
                }
            }
            "X" => {
                self.msgs.entry(num(1)).or_default().push(Msg { ts: num(2), user: g(3).into(), text: g(4).into() });
            }
            "D" => {
                self.dms
                    .entry(dm_key(g(1), g(2)))
                    .or_default()
                    .push(Msg { ts: num(3), user: g(4).into(), text: g(5).into() });
            }
            _ => {}
        }
    }

    fn reply(&self, cid: u64, fields: &[&str]) {
        if let Some(c) = self.conns.get(&cid) {
            let _ = c.tx.send(encode(fields));
        }
    }

    fn send_user(&self, user: &str, fields: &[&str]) {
        let line = encode(fields);
        for c in self.conns.values() {
            if c.user.as_deref() == Some(user) {
                let _ = c.tx.send(line.clone());
            }
        }
    }

    fn broadcast_server(&self, sid: u64, fields: &[&str]) {
        if let Some(s) = self.servers.get(&sid) {
            let line = encode(fields);
            let set: HashSet<&String> = s.members.iter().collect();
            for c in self.conns.values() {
                if let Some(u) = &c.user {
                    if set.contains(u) {
                        let _ = c.tx.send(line.clone());
                    }
                }
            }
        }
    }

    fn is_online(&self, user: &str) -> bool {
        self.conns.values().any(|c| c.user.as_deref() == Some(user))
    }

    fn is_member(&self, sid: u64, user: &str) -> bool {
        self.servers.get(&sid).map(|s| s.members.iter().any(|m| m == user)).unwrap_or(false)
    }

    fn co_members(&self, user: &str) -> HashSet<String> {
        let mut out = HashSet::new();
        for s in self.servers.values() {
            if s.members.iter().any(|m| m == user) {
                out.extend(s.members.iter().filter(|m| *m != user).cloned());
            }
        }
        out
    }

    fn presence(&self, user: &str, on: bool) {
        for other in self.co_members(user) {
            self.send_user(&other, &["PRESENCE", user, if on { "on" } else { "off" }]);
        }
    }

    fn send_server_info(&self, cid: u64, sid: u64) {
        if let Some(s) = self.servers.get(&sid) {
            self.reply(cid, &["SERVER", &sid.to_string(), &s.name, &s.invite, &s.owner]);
        }
    }

    fn voice_leave(&mut self, user: &str) {
        let mut left: Option<u64> = None;
        for (chan, members) in self.voice_members.iter_mut() {
            if let Some(pos) = members.iter().position(|m| m == user) {
                members.remove(pos);
                left = Some(*chan);
                break;
            }
        }
        self.voice_tokens.retain(|_, t| t.user != user);
        if let Some(chan) = left {
            if let Some(sid) = self.channels.get(&chan).map(|c| c.server) {
                self.broadcast_server(sid, &["VOICE_STATE", &chan.to_string(), user, "left"]);
            }
        }
    }

    fn disconnect(&mut self, cid: u64) {
        let user = self.conns.remove(&cid).and_then(|c| c.user);
        if let Some(u) = user {
            if !self.is_online(&u) {
                self.voice_leave(&u);
                self.presence(&u, false);
            }
        }
    }
}

type Shared = Arc<Mutex<State>>;

/// Retorna `false` para encerrar a conexão.
fn handle(st: &mut State, cid: u64, f: Vec<String>) -> bool {
    let cmd = f[0].to_uppercase();
    let arg = |i: usize| f.get(i).map(|s| s.as_str()).unwrap_or("");
    let num = |i: usize| arg(i).parse::<u64>().unwrap_or(0);
    let user = st.conns.get(&cid).and_then(|c| c.user.clone());

    match cmd.as_str() {
        "QUIT" => return false,
        "PING" => {
            st.reply(cid, &["PONG"]);
            return true;
        }
        "REGISTER" => {
            let (u, p) = (arg(1), arg(2));
            if user.is_some() {
                st.reply(cid, &["ERR", "Você já está logado."]);
            } else if !valid_username(u) {
                st.reply(cid, &["ERR", "Usuário inválido: use 2 a 20 letras, números ou _."]);
            } else if p.chars().count() < 6 {
                st.reply(cid, &["ERR", "A senha precisa ter pelo menos 6 caracteres."]);
            } else if st.users.keys().any(|k| k.eq_ignore_ascii_case(u)) {
                st.reply(cid, &["ERR", "Esse usuário já existe."]);
            } else {
                let salt = crypto::rand_bytes(16);
                let hash = crypto::pbkdf2(p.as_bytes(), &salt, PBKDF2_ITERS);
                st.persist(&["U", u, &crypto::hex(&salt), &crypto::hex(&hash)]);
                st.users.insert(u.to_string(), UserRec { salt: salt.clone(), hash: hash.to_vec() });
                login(st, cid, u);
            }
            return true;
        }
        "LOGIN" => {
            let (u, p) = (arg(1), arg(2));
            if user.is_some() {
                st.reply(cid, &["ERR", "Você já está logado."]);
                return true;
            }
            let ok = match st.users.get(u) {
                Some(rec) => crypto::ct_eq(&crypto::pbkdf2(p.as_bytes(), &rec.salt, PBKDF2_ITERS), &rec.hash),
                None => {
                    // gasta o mesmo tempo para não revelar se o usuário existe
                    let _ = crypto::pbkdf2(p.as_bytes(), b"0000000000000000", PBKDF2_ITERS);
                    false
                }
            };
            if ok {
                login(st, cid, u);
            } else {
                st.reply(cid, &["ERR", "Usuário ou senha incorretos."]);
            }
            return true;
        }
        _ => {}
    }

    let Some(me) = user else {
        st.reply(cid, &["ERR", "Faça login primeiro (/login ou /register)."]);
        return true;
    };

    match cmd.as_str() {
        "SERVER_CREATE" => {
            let name = clean_name(arg(1), 50);
            if name.is_empty() {
                st.reply(cid, &["ERR", "Nome de servidor inválido."]);
                return true;
            }
            let sid = st.alloc_id();
            let invite: String = {
                const AL: &[u8] = b"abcdefghijkmnpqrstuvwxyz23456789";
                crypto::rand_bytes(8).iter().map(|b| AL[(*b as usize) % AL.len()] as char).collect()
            };
            st.persist(&["S", &sid.to_string(), &name, &me, &invite]);
            st.persist(&["M", &sid.to_string(), &me]);
            st.servers.insert(
                sid,
                Srv { name, owner: me.clone(), invite, members: vec![me.clone()], channels: vec![] },
            );
            for (kind, cname) in [(Kind::Text, "geral"), (Kind::Voice, "Voz")] {
                add_channel(st, sid, kind, cname.to_string());
            }
            st.send_server_info(cid, sid);
            send_channels(st, cid, sid);
        }
        "SERVER_JOIN" => {
            let code = arg(1).trim().to_lowercase();
            let found = st.servers.iter().find(|(_, s)| s.invite == code).map(|(id, _)| *id);
            match found {
                None => st.reply(cid, &["ERR", "Convite inválido."]),
                Some(sid) => {
                    if !st.is_member(sid, &me) {
                        st.persist(&["M", &sid.to_string(), &me]);
                        st.servers.get_mut(&sid).unwrap().members.push(me.clone());
                        st.broadcast_server(sid, &["MEMBER_JOIN", &sid.to_string(), &me]);
                    }
                    st.send_server_info(cid, sid);
                    send_channels(st, cid, sid);
                }
            }
        }
        "SERVERS" => {
            let ids: Vec<u64> = st.servers.iter().filter(|(_, s)| s.members.contains(&me)).map(|(i, _)| *i).collect();
            for sid in ids {
                st.send_server_info(cid, sid);
            }
            st.reply(cid, &["END", "SERVERS"]);
        }
        "CHANNELS" => {
            let sid = num(1);
            if !st.is_member(sid, &me) {
                st.reply(cid, &["ERR", "Você não está nesse servidor."]);
            } else {
                send_channels(st, cid, sid);
            }
        }
        "CHANNEL_CREATE" => {
            let sid = num(1);
            let kind = Kind::parse(arg(2));
            let owner_ok = st.servers.get(&sid).map(|s| s.owner == me).unwrap_or(false);
            let name = clean_name(arg(3), 30);
            if !owner_ok {
                st.reply(cid, &["ERR", "Só o dono do servidor pode criar canais."]);
            } else if kind.is_none() || name.is_empty() {
                st.reply(cid, &["ERR", "Uso: /newchannel text|voice <nome>"]);
            } else {
                let kind = kind.unwrap();
                let name = if kind == Kind::Text { name.to_lowercase().replace(' ', "-") } else { name };
                add_channel(st, sid, kind, name);
            }
        }
        "MEMBERS" => {
            let sid = num(1);
            if !st.is_member(sid, &me) {
                st.reply(cid, &["ERR", "Você não está nesse servidor."]);
            } else {
                let members = st.servers[&sid].members.clone();
                for m in members {
                    let on = if st.is_online(&m) { "1" } else { "0" };
                    st.reply(cid, &["MEMBER", &sid.to_string(), &m, on]);
                }
                st.reply(cid, &["END", "MEMBERS"]);
            }
        }
        "SEND" => {
            let ch = num(1);
            let text = arg(2).trim();
            let info = st.channels.get(&ch).map(|c| (c.server, c.kind));
            match info {
                Some((sid, Kind::Text)) if st.is_member(sid, &me) => {
                    if text.is_empty() || text.chars().count() > MAX_MSG_LEN {
                        st.reply(cid, &["ERR", "Mensagem vazia ou maior que 2000 caracteres."]);
                    } else {
                        let ts = now();
                        st.persist(&["X", &ch.to_string(), &ts.to_string(), &me, text]);
                        st.msgs.entry(ch).or_default().push(Msg { ts, user: me.clone(), text: text.to_string() });
                        st.broadcast_server(sid, &["MSG", &ch.to_string(), &ts.to_string(), &me, text]);
                    }
                }
                _ => st.reply(cid, &["ERR", "Canal de texto não encontrado."]),
            }
        }
        "HISTORY" => {
            let ch = num(1);
            let n = (num(2).max(1)).min(200) as usize;
            let info = st.channels.get(&ch).map(|c| (c.server, c.kind));
            match info {
                Some((sid, Kind::Text)) if st.is_member(sid, &me) => {
                    let list = st.msgs.get(&ch).cloned().unwrap_or_default();
                    let start = list.len().saturating_sub(n);
                    for m in &list[start..] {
                        st.reply(cid, &["HIST", &ch.to_string(), &m.ts.to_string(), &m.user, &m.text]);
                    }
                    st.reply(cid, &["END", "HISTORY"]);
                }
                _ => st.reply(cid, &["ERR", "Canal de texto não encontrado."]),
            }
        }
        "DM" => {
            let to = arg(1);
            let text = arg(2).trim();
            if !st.users.contains_key(to) {
                st.reply(cid, &["ERR", "Usuário não existe."]);
            } else if text.is_empty() || text.chars().count() > MAX_MSG_LEN {
                st.reply(cid, &["ERR", "Mensagem vazia ou maior que 2000 caracteres."]);
            } else {
                let ts = now();
                st.persist(&["D", &me, to, &ts.to_string(), &me, text]);
                st.dms.entry(dm_key(&me, to)).or_default().push(Msg { ts, user: me.clone(), text: text.to_string() });
                let line = ["DM", &ts.to_string(), &me, to, text];
                st.send_user(&me, &line);
                if to != me {
                    st.send_user(to, &line);
                }
            }
        }
        "DM_HISTORY" => {
            let other = arg(1);
            let n = (num(2).max(1)).min(200) as usize;
            let list = st.dms.get(&dm_key(&me, other)).cloned().unwrap_or_default();
            let start = list.len().saturating_sub(n);
            for m in &list[start..] {
                let to = if m.user == me { other } else { me.as_str() };
                st.reply(cid, &["DMHIST", &m.ts.to_string(), &m.user, to, &m.text]);
            }
            st.reply(cid, &["END", "DMHIST"]);
        }
        "VOICE_JOIN" => {
            let ch = num(1);
            let info = st.channels.get(&ch).map(|c| (c.server, c.kind));
            match info {
                Some((sid, Kind::Voice)) if st.is_member(sid, &me) => {
                    st.voice_leave(&me);
                    let token = crypto::rand_u64();
                    st.voice_tokens.insert(token, VoiceTok { user: me.clone(), chan: ch, addr: None });
                    st.voice_members.entry(ch).or_default().push(me.clone());
                    st.reply(cid, &["VOICE_OK", &ch.to_string(), &st.udp_port.to_string(), &token.to_string()]);
                    st.broadcast_server(sid, &["VOICE_STATE", &ch.to_string(), &me, "joined"]);
                }
                _ => st.reply(cid, &["ERR", "Canal de voz não encontrado."]),
            }
        }
        "VOICE_LEAVE" => st.voice_leave(&me),
        _ => st.reply(cid, &["ERR", "Comando desconhecido."]),
    }
    true
}

fn login(st: &mut State, cid: u64, user: &str) {
    let first = !st.is_online(user);
    if let Some(c) = st.conns.get_mut(&cid) {
        c.user = Some(user.to_string());
    }
    st.reply(cid, &["AUTH_OK", user]);
    if first {
        st.presence(user, true);
    }
}

fn add_channel(st: &mut State, sid: u64, kind: Kind, name: String) {
    let ch = st.alloc_id();
    st.persist(&["C", &ch.to_string(), &sid.to_string(), kind.as_str(), &name]);
    st.channels.insert(ch, Chan { server: sid, kind, name: name.clone() });
    if let Some(s) = st.servers.get_mut(&sid) {
        s.channels.push(ch);
    }
    st.broadcast_server(sid, &["CHANNEL", &sid.to_string(), &ch.to_string(), kind.as_str(), &name]);
}

fn send_channels(st: &State, cid: u64, sid: u64) {
    if let Some(s) = st.servers.get(&sid) {
        for ch in &s.channels {
            if let Some(c) = st.channels.get(ch) {
                st.reply(cid, &["CHANNEL", &sid.to_string(), &ch.to_string(), c.kind.as_str(), &c.name]);
                if let Some(vm) = st.voice_members.get(ch) {
                    for u in vm {
                        st.reply(cid, &["VOICE_STATE", &ch.to_string(), u, "joined"]);
                    }
                }
            }
        }
    }
    st.reply(cid, &["END", "CHANNELS"]);
}

fn client_thread(stream: TcpStream, shared: Shared) {
    let (tx, rx) = channel::<String>();
    let cid = {
        let mut st = shared.lock().unwrap();
        let id = st.alloc_id();
        st.conns.insert(id, Conn { user: None, tx });
        st.reply(id, &["HELLO", "rustcord", "1"]);
        id
    };
    let mut writer = match stream.try_clone() {
        Ok(w) => w,
        Err(_) => {
            shared.lock().unwrap().disconnect(cid);
            return;
        }
    };
    thread::spawn(move || {
        for line in rx {
            if writer.write_all(line.as_bytes()).is_err() || writer.write_all(b"\n").is_err() || writer.flush().is_err() {
                break;
            }
        }
        let _ = writer.shutdown(std::net::Shutdown::Both);
    });

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if line.len() > MAX_LINE {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        let fields = decode(trimmed);
        let mut st = shared.lock().unwrap();
        if !handle(&mut st, cid, fields) {
            break;
        }
    }
    shared.lock().unwrap().disconnect(cid);
}

fn udp_thread(sock: UdpSocket, shared: Shared) {
    let mut buf = [0u8; 4096];
    loop {
        let (n, addr) = match sock.recv_from(&mut buf) {
            Ok(x) => x,
            Err(_) => continue,
        };
        if n < 8 {
            continue;
        }
        let token = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let payload = &buf[8..n];
        let (name, targets) = {
            let mut st = shared.lock().unwrap();
            let (user, chan) = match st.voice_tokens.get_mut(&token) {
                Some(t) => {
                    t.addr = Some(addr);
                    (t.user.clone(), t.chan)
                }
                None => continue,
            };
            if payload.is_empty() {
                continue;
            }
            let targets: Vec<SocketAddr> = st
                .voice_tokens
                .iter()
                .filter(|(k, t)| **k != token && t.chan == chan)
                .filter_map(|(_, t)| t.addr)
                .collect();
            (user, targets)
        };
        let nb = name.as_bytes();
        let mut out = Vec::with_capacity(1 + nb.len() + payload.len());
        out.push(nb.len() as u8);
        out.extend_from_slice(nb);
        out.extend_from_slice(payload);
        for t in targets {
            let _ = sock.send_to(&out, t);
        }
    }
}

/// Inicia o servidor em threads de fundo e retorna o endereço TCP real.
pub fn start(bind: &str, data: Option<PathBuf>) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(bind)?;
    let addr = listener.local_addr()?;
    let udp = UdpSocket::bind(SocketAddr::new(addr.ip(), addr.port()))?;

    let mut state = State::new();
    state.udp_port = addr.port();
    if let Some(p) = data {
        state.load(&p)?;
    }
    let shared: Shared = Arc::new(Mutex::new(state));

    {
        let s = shared.clone();
        thread::spawn(move || udp_thread(udp, s));
    }
    {
        let s = shared.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _ = stream.set_nodelay(true);
                let s2 = s.clone();
                thread::spawn(move || client_thread(stream, s2));
            }
        });
    }
    Ok(addr)
}

/// Inicia e bloqueia para sempre.
pub fn serve(bind: &str, data: Option<PathBuf>) -> std::io::Result<()> {
    let addr = start(bind, data)?;
    println!("Rustcord server ouvindo em {addr} (TCP texto + UDP voz)");
    loop {
        thread::park();
    }
}
