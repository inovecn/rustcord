use rustcord_proto::{decode, encode};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, UdpSocket};
use std::time::Duration;

struct Cli {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

impl Cli {
    fn connect(addr: std::net::SocketAddr) -> Cli {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let r = BufReader::new(s.try_clone().unwrap());
        let mut c = Cli { w: s, r };
        c.until("HELLO");
        c
    }
    fn send(&mut self, f: &[&str]) {
        writeln!(self.w, "{}", encode(f)).unwrap();
    }
    /// Lê linhas até achar uma que começa com `first`.
    fn until(&mut self, first: &str) -> Vec<String> {
        loop {
            let mut l = String::new();
            let n = self.r.read_line(&mut l).expect("timeout esperando resposta");
            assert!(n > 0, "conexão fechada esperando {first}");
            let f = decode(&l);
            if f[0] == first {
                return f;
            }
        }
    }
}

#[test]
fn fluxo_completo() {
    let addr = rustcord_server::start("127.0.0.1:0", None).unwrap();
    let mut alice = Cli::connect(addr);
    let mut bob = Cli::connect(addr);

    // sem login
    alice.send(&["SERVERS"]);
    assert_eq!(alice.until("ERR")[0], "ERR");

    // cadastro / login
    alice.send(&["REGISTER", "alice", "123"]);
    assert!(alice.until("ERR")[1].contains("6 caracteres"));
    alice.send(&["REGISTER", "alice", "senha123"]);
    assert_eq!(alice.until("AUTH_OK")[1], "alice");
    bob.send(&["REGISTER", "bob", "outrasenha"]);
    bob.until("AUTH_OK");
    bob.send(&["LOGIN", "x", "y"]);
    assert!(bob.until("ERR")[1].contains("já está logado"));

    // servidor + convite
    alice.send(&["SERVER_CREATE", "Meu Servidor"]);
    let srv = alice.until("SERVER");
    let (sid, invite) = (srv[1].clone(), srv[3].clone());
    let mut text_ch = String::new();
    let mut voice_ch = String::new();
    loop {
        let c = alice.until("CHANNEL");
        if c[3] == "text" {
            text_ch = c[2].clone();
        } else {
            voice_ch = c[2].clone();
        }
        if !text_ch.is_empty() && !voice_ch.is_empty() {
            break;
        }
    }
    bob.send(&["SERVER_JOIN", "codigo-ruim"]);
    assert!(bob.until("ERR")[1].contains("Convite"));
    bob.send(&["SERVER_JOIN", &invite]);
    assert_eq!(bob.until("SERVER")[1], sid);
    bob.until("END");

    // mensagem de texto chega para os dois, com caracteres especiais
    alice.send(&["SEND", &text_ch, "oi bob\tcom tab\ne linha 2"]);
    let m = bob.until("MSG");
    assert_eq!(m[3], "alice");
    assert_eq!(m[4], "oi bob\tcom tab\ne linha 2");
    alice.until("MSG");

    // histórico
    bob.send(&["HISTORY", &text_ch, "10"]);
    assert_eq!(bob.until("HIST")[3], "alice");

    // DM
    bob.send(&["DM", "alice", "psiu"]);
    assert_eq!(alice.until("DM")[4], "psiu");
    bob.until("DM");
    alice.send(&["DM_HISTORY", "bob", "10"]);
    assert_eq!(alice.until("DMHIST")[4], "psiu");

    // só o dono cria canais
    bob.send(&["CHANNEL_CREATE", &sid, "text", "x"]);
    bob.until("ERR");
    alice.send(&["CHANNEL_CREATE", &sid, "text", "Off Topic"]);
    assert_eq!(alice.until("CHANNEL")[4], "off-topic");

    // voz: tokens + relay UDP
    alice.send(&["VOICE_JOIN", &voice_ch]);
    let a = alice.until("VOICE_OK");
    bob.send(&["VOICE_JOIN", &voice_ch]);
    let b = bob.until("VOICE_OK");
    let port: u16 = a[2].parse().unwrap();
    let ta: u64 = a[3].parse().unwrap();
    let tb: u64 = b[3].parse().unwrap();

    let ua = UdpSocket::bind("127.0.0.1:0").unwrap();
    let ub = UdpSocket::bind("127.0.0.1:0").unwrap();
    ub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    ua.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let dest = ("127.0.0.1", port);
    ua.send_to(&ta.to_be_bytes(), dest).unwrap(); // hello
    ub.send_to(&tb.to_be_bytes(), dest).unwrap();
    std::thread::sleep(Duration::from_millis(200));

    let mut pkt = ta.to_be_bytes().to_vec();
    pkt.extend_from_slice(&[7u8; 640]);
    ua.send_to(&pkt, dest).unwrap();
    let mut buf = [0u8; 2048];
    let (n, _) = ub.recv_from(&mut buf).expect("bob não recebeu voz");
    assert_eq!(buf[0] as usize, 5);
    assert_eq!(&buf[1..6], b"alice");
    assert_eq!(n, 1 + 5 + 640);
    // quem fala não recebe o próprio áudio
    assert!(ua.recv_from(&mut buf).is_err());

    // token inválido é ignorado
    let mut bad = 12345u64.to_be_bytes().to_vec();
    bad.extend_from_slice(&[1u8; 640]);
    ua.send_to(&bad, dest).unwrap();
    ub.set_read_timeout(Some(Duration::from_millis(400))).unwrap();
    assert!(ub.recv_from(&mut buf).is_err());

    // sair da voz avisa o servidor
    bob.send(&["VOICE_LEAVE"]);
    let st = alice.until("VOICE_STATE");
    assert!(st[3] == "joined" || st[3] == "left");
}

#[test]
fn persistencia() {
    let path = std::env::temp_dir().join(format!("rustcord-test-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let addr = rustcord_server::start("127.0.0.1:0", Some(path.clone())).unwrap();
    let mut c = Cli::connect(addr);
    c.send(&["REGISTER", "carol", "senhaboa1"]);
    c.until("AUTH_OK");
    c.send(&["SERVER_CREATE", "Persistente"]);
    let ch = c.until("CHANNEL");
    c.send(&["SEND", &ch[2], "lembra de mim"]);
    c.until("MSG");

    // segundo servidor lendo o mesmo arquivo
    let addr2 = rustcord_server::start("127.0.0.1:0", Some(path.clone())).unwrap();
    let mut d = Cli::connect(addr2);
    d.send(&["LOGIN", "carol", "errada"]);
    d.until("ERR");
    d.send(&["LOGIN", "carol", "senhaboa1"]);
    d.until("AUTH_OK");
    d.send(&["SERVERS"]);
    assert_eq!(d.until("SERVER")[2], "Persistente");
    d.send(&["HISTORY", &ch[2], "5"]);
    assert_eq!(d.until("HIST")[4], "lembra de mim");
    let _ = std::fs::remove_file(&path);
}
