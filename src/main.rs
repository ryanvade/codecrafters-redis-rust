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

    loop {
        let mut buf = vec![0; 1024];
        match socket.read(&mut buf).await {
            Ok(n) => {
                if n != 0 {
                    let s = match str::from_utf8(&buf) {
                        Ok(v) => v,
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    };

                    eprintln!("received {:?}", s);

                    socket.write_all(b"+PONG\r\n").await.expect("cannot write pong response");

                    socket.flush().await.expect("cannot flush socket");
                }
            },
            Err(_) => {
                break
            }
        }
    }
    eprint!("end of process_request")
}
