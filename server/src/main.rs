use std::path::PathBuf;

fn main() {
    let bind = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:7878".to_string());
    let data = std::env::args().nth(2).unwrap_or_else(|| "rustcord.db".to_string());
    println!("Dados salvos em: {data}");
    if let Err(e) = rustcord_server::serve(&bind, Some(PathBuf::from(data))) {
        eprintln!("Erro ao iniciar o servidor: {e}");
        std::process::exit(1);
    }
}
