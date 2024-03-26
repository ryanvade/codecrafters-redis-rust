use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::str;

#[tokio::main]
async fn main() {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("0.0.0.0:6379").await.expect("cannot listen on port 6379");

    loop {
        let (socket, _) = listener.accept().await.expect("cannot accept connections");
        tokio::spawn(async move {
            process_request(socket).await;
        });
    }
}


async fn process_request(mut socket: TcpStream) {
    eprintln!("accepted new connection");

    let mut buf = vec![0; 16];
    let n = socket.read(&mut buf).await.expect("cannot read from socket");
    if n == 0 {
        return;
    }

    let s = match str::from_utf8(&buf) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };

    eprintln!("received {:?}", s);

    if s.to_lowercase().contains("ping") {
        socket.write_all(b"+PONG\r\n").await.expect("cannot write pong response");
    }

    socket.flush().await.expect("cannot flush");
    socket.shutdown().await.expect("cannot shutdown");
}
