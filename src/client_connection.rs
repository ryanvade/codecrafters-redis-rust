use crate::data_core::{Command, DataCore};
use crate::server::ReplicationSettings;
use crate::{parser, tokenizer};
use std::fmt;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct ClientConnection {
    tcp_stream: TcpStream,
    peer_addr: SocketAddr,
}

impl ClientConnection {
    pub fn new(tcp_stream: TcpStream, peer_addr: SocketAddr) -> ClientConnection {
        ClientConnection {
            tcp_stream,
            peer_addr,
        }
    }

    pub async fn handle_requests(
        &mut self,
        data_core_arc: Arc<Mutex<DataCore>>,
        replication_settings: ReplicationSettings,
    ) {
        loop {
            let mut buf = vec![0; 1024];
            match self.tcp_stream.read(&mut buf).await {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }

                    let s = match std::str::from_utf8(&buf[..n]) {
                        Ok(v) => v,
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    };

                    eprintln!("received {:?}", s);

                    let tokens =
                        tokenizer::parse_resp_tokens_from_str(s).expect("cannot tokenize request");
                    eprintln!("Tokens: {:?}", tokens);

                    let parser_value =
                        parser::parse_tokens(&tokens).expect("cannot parse values from tokens");
                    eprintln!("Parser Value: {:?}", parser_value);

                    if !parser_value.is_array() {
                        eprintln!("Parent parser value is not an array, exiting");
                        self.tcp_stream
                            .shutdown()
                            .await
                            .expect("unable to shutdown tcpstream");
                        break;
                    }

                    let parser_values = parser_value
                        .to_vec()
                        .expect("could not get vec of parser values");

                    let command = Command::new(
                        Arc::new(parser_values.clone()),
                        replication_settings.clone(),
                    );
                    let is_psync = command.is_psync();

                    let mut guard = data_core_arc.as_ref().lock().await;
                    let data_core = guard.deref_mut();
                    let response = data_core.process_command(command).await;

                    let response = tokenizer::serialize_tokens(&response)
                        .expect("cannot serialize response tokens");

                    self.tcp_stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("cannot write response to tcpstream");

                    if is_psync {
                        let rdb_bytes = data_core.to_rdb_bytes();
                        self.tcp_stream
                            .write_all(&rdb_bytes)
                            .await
                            .expect("cannot write to psync rdb tcpstream");
                    }

                    self.tcp_stream.flush().await.expect("cannot flush socket");
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                    break;
                }
            }
        }
    }
}

impl fmt::Display for ClientConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Client connected to {}", self.peer_addr)
    }
}
