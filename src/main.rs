use std::str;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::parser::ParserValue;
use redis_starter_rust::{parser, tokenizer};

#[tokio::main]
async fn main() {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("0.0.0.0:6379")
        .await
        .expect("cannot listen on port 6379");

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

                    let parser_values = parser_value
                        .to_vec()
                        .expect("could not get vec of parser values");
                    let command = parser_values
                        .first()
                        .expect("parser values does not have a first item");

                    match command.to_string().unwrap().to_lowercase().as_str() {
                        "ping" => {
                            let parser_value = ParserValue::SimpleString(String::from("PONG"));
                            let response_tokens = parser_value.to_tokens();
                            eprintln!("PING response_tokens {:?}", response_tokens);
                            let response = tokenizer::serialize_tokens(&response_tokens)
                                .expect("cannot serialize response tokens");
                            socket
                                .write_all(response.as_bytes())
                                .await
                                .expect("cannot write response to tcpstream");
                        }
                        "echo" => {
                            let mut tokens: Vec<tokenizer::Token> = Vec::new();
                            let mut iter = parser_values.iter();
                            let _ = iter.next();
                            // TODO: how to handle multiple strings passed to echo?
                            while let Some(echo_str_token) = iter.next() {
                                if let Some(echo_str) = echo_str_token.to_string() {
                                    let parser_value = ParserValue::BulkString(echo_str);
                                    let mut response_tokens = parser_value.to_tokens();
                                    tokens.append(&mut response_tokens);
                                }
                            }
                            let response = tokenizer::serialize_tokens(&tokens)
                                .expect("cannot serialize response tokens");
                            socket
                                .write_all(response.as_bytes())
                                .await
                                .expect("cannot write response to tcpstream");
                        }
                        _ => todo!(),
                    }

                    socket.flush().await.expect("cannot flush socket");
                }
            }
            Err(_) => break,
        }
    }
    eprint!("end of process_request")
}
