use std::str;
use std::sync::Arc;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};

use redis_starter_rust::data_core::{Command, ReplicationRole};
use redis_starter_rust::tokenizer::Token;
use redis_starter_rust::{data_core, parser, tokenizer};

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "6379")]
    port: u64,

    #[arg(short, long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    eprintln!("Logs from your program will appear here!");

    let args = Args::parse();

    let mut replication_role = ReplicationRole::Master;
    let mut master_host: Option<String> = None;
    let mut master_port: Option<u64> = None;

    if let Some(replica_of) = args.replicaof {
        eprintln!("Replica of {}", replica_of);
        replication_role = ReplicationRole::Slave;
        let (master_host_str, master_host_port_str) = replica_of
            .split_once(' ')
            .expect("replica_of split should have two values");
        master_host = Some(master_host_str.to_string());
        master_port = Some(master_host_port_str.parse::<u64>().unwrap());
    }

    let (tx, rx) = mpsc::channel::<Command>(32);

    let mut data_core = data_core::DataCore::new(rx, replication_role, master_host, master_port);

    if data_core.is_slave() {
        data_core
            .initialize_slaves(args.port)
            .await
            .expect("should be able to initialize slaves");
    }

    let _ = tokio::spawn(async move {
        data_core.process_command().await;
    });

    let addr = format!("0.0.0.0:{}", args.port.to_string());

    let listener = TcpListener::bind(addr)
        .await
        .expect("cannot listen on port 6379");

    loop {
        let tx = tx.clone();
        let (socket, _) = listener.accept().await.expect("cannot accept connections");
        tokio::spawn(async move {
            process_request(socket, &tx).await;
        });
    }
}

async fn process_request<'c>(mut socket: TcpStream, core_tx: &Sender<Command>) {
    eprintln!("accepted new connection");

    loop {
        let mut buf = vec![0; 1024];
        match socket.read(&mut buf).await {
            Ok(n) => {
                if n != 0 {
                    let s = match str::from_utf8(&buf[..n]) {
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
                        socket
                            .shutdown()
                            .await
                            .expect("unable to shutdown tcpstream");
                        break;
                    }

                    let (tx, rx) = oneshot::channel::<Vec<Token>>();

                    let parser_values = parser_value
                        .to_vec()
                        .expect("could not get vec of parser values");

                    let command = Command::new(Arc::new(parser_values.clone()), tx);
                    core_tx
                        .send(command)
                        .await
                        .expect("should be able to send commands to data core");

                    let response = rx
                        .await
                        .expect("should be able to receive a response from data core");

                    let response = tokenizer::serialize_tokens(&response)
                        .expect("cannot serialize response tokens");

                    socket
                        .write_all(response.as_bytes())
                        .await
                        .expect("cannot write response to tcpstream");
                    socket.flush().await.expect("cannot flush socket");
                }
            }
            Err(_) => break,
        }
    }
    eprint!("end of process_request")
}
