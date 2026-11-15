# Rustcord

Um "Discord" feito em Rust, **sem nenhuma dependência externa** (só a biblioteca padrão).

- **Servidor**: contas (senha com PBKDF2-HMAC-SHA256 + salt), servidores com convite, canais de texto e voz,
  mensagens com histórico, DMs, presença online/offline, relay de voz por UDP, persistência em arquivo.
- **Cliente desktop**: app de terminal com comandos `/`, notificações de todos os canais e voz com mixagem.
- **Protocolo**: texto separado por TAB (TCP) + pacotes PCM (UDP). Veja `proto/src/lib.rs`.

## Rodando

```bash
cargo run --release -p rustcord-server            # 0.0.0.0:7878, dados em ./rustcord.db
cargo run --release -p rustcord-server -- 0.0.0.0:9000 meus-dados.db

cargo run --release -p rustcord-client            # 127.0.0.1:7878
cargo run --release -p rustcord-client -- 192.168.0.10:7878
```

Libere a porta **TCP e UDP** (a mesma) no firewall se for jogar fora da sua máquina.

## Uso rápido

```
/register eric minhasenha
/create Meu Servidor        -> mostra o código de convite
/go geral
oi pessoal                  -> texto normal vai pro canal atual
/dm amigo e aí?
/voice Voz
/mute   /leave
```

Outro usuário entra com `/join <convite>`. `/help` lista tudo.

## Voz

Sem crates não há acesso direto ao microfone, então o cliente usa um processo externo que lê/escreve
PCM cru (s16le, 16 kHz, mono):

- **Linux**: automático com `arecord`/`aplay` (pacote `alsa-utils`).
- **Windows/macOS**: defina as variáveis de ambiente, por exemplo com ffmpeg:

```
RUSTCORD_CAPTURE_CMD=ffmpeg -loglevel quiet -f dshow -i audio="NOME DO MICROFONE" -f s16le -ar 16000 -ac 1 -
RUSTCORD_PLAYBACK_CMD=ffplay -nodisp -loglevel quiet -f s16le -ar 16000 -ac 1 -i -
```

## Testes

`cargo test` roda testes unitários (SHA-256/HMAC/PBKDF2 com vetores oficiais) e testes ponta a ponta:
cadastro, convite, mensagens, DM, permissões, relay de voz UDP e persistência.

## Limitações atuais

- Cliente é de terminal (sem janela gráfica); a voz não tem codec (PCM puro, ~256 kbps por falante)
  nem cancelamento de eco. Use fones.
- Sem TLS: a conexão e a senha trafegam sem criptografia. Use em rede local/VPN ou atrás de um túnel.
- Sem papéis/permissões além de "dono do servidor", sem edição/exclusão de mensagens, sem anexos.
