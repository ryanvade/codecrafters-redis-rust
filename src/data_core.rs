use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;

use crate::parser::ParserValue;
use crate::tokenizer;
use crate::tokenizer::Token;

#[derive(Debug)]
pub struct Command {
    pub arguments: Arc<Vec<ParserValue>>,
    pub response_channel: Sender<Vec<Token>>,
}

impl Command {
    pub fn new(arguments: Arc<Vec<ParserValue>>, response_channel: Sender<Vec<Token>>) -> Command {
        Command {
            arguments,
            response_channel,
        }
    }
}

#[derive(Debug)]
pub struct DataCore {
    data_set: HashMap<String, ParserValue>,
    rx: Receiver<Command>,
}

impl DataCore {
    pub fn new(rx: Receiver<Command>) -> DataCore {
        DataCore {
            data_set: HashMap::new(),
            rx,
        }
    }

    pub async fn process_command(self: &mut DataCore) {
        while let Some(command) = self.rx.recv().await {
            eprintln!("Process Command {:?}", command);
            let first = command
                .arguments
                .first()
                .expect("arguments should have at least one argument");
            match first.to_string().unwrap().to_lowercase().as_str() {
                "ping" => {
                    let parser_value = ParserValue::SimpleString(String::from("PONG"));
                    let response = parser_value.to_tokens();
                    eprintln!("PING response_tokens {:?}", response);
                    command.response_channel.send(response).unwrap();
                }
                "echo" => {
                    let mut tokens: Vec<tokenizer::Token> = Vec::new();
                    let mut iter = command.arguments.iter();
                    let _ = iter.next();
                    // TODO: how to handle multiple strings passed to echo?
                    while let Some(echo_str_token) = iter.next() {
                        if let Some(echo_str) = echo_str_token.to_string() {
                            let parser_value = ParserValue::BulkString(echo_str);
                            let mut response_tokens = parser_value.to_tokens();
                            tokens.append(&mut response_tokens);
                        }
                    }
                    command.response_channel.send(tokens).unwrap();
                }
                "set" => {
                    let mut iter = command.arguments.iter().peekable();
                    let _ = iter.next();
                    let key = iter.next().expect("set command should have a key");
                    let value = iter.next().expect("set command should have a value");
                    eprintln!("Key: {:?}", key);
                    eprintln!("Value: {:?}", value);

                    if key.is_string() {
                        let key = key
                            .to_string()
                            .expect("string parser value should be convertable to string");
                        let res = self.data_set.insert(key.clone(), value.clone());
                        if res.is_some() {
                            let res = res.expect("some value should exist");
                            eprintln!("Previous Value: {:?}", res);
                        }

                        if iter.peek().is_some_and(|pv| pv.is_string()) {
                            let px = iter.next().unwrap().to_string().unwrap();
                            if iter.peek().is_some_and(|len| len.is_string()) {
                                let len = iter.next().unwrap().to_string().unwrap();
                                let len = len.parse::<i64>().expect("len string should be i64");
                                eprintln!("{:?} {:?}", px, len);
                            }
                        }
                    }
                    let parser_value = ParserValue::SimpleString(String::from("OK"));
                    let response_tokens = parser_value.to_tokens();
                    eprintln!("SET response_tokens {:?}", response_tokens);
                    command.response_channel.send(response_tokens).unwrap();
                }
                "get" => {
                    let mut iter = command.arguments.iter();
                    let _ = iter.next();
                    let key = iter.next().expect("set command should have a key");
                    if key.is_string() {
                        let key = key
                            .to_string()
                            .expect("string parser value should be convertable to a string");
                        let value = self.data_set.get(&key);
                        if let Some(value) = value {
                            command.response_channel.send(value.to_tokens()).unwrap();
                        } else {
                            let response_value = ParserValue::NullBulkString;
                            command
                                .response_channel
                                .send(response_value.to_tokens())
                                .unwrap()
                        }
                    } else {
                        // TODO: return error message here
                    }
                }
                "command" => {
                    let parser_value = ParserValue::SimpleString(String::from(""));
                    let response = parser_value.to_tokens();
                    eprintln!("COMMAND response_tokens {:?}", response);
                    command.response_channel.send(response).unwrap();
                }
                _ => todo!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{mpsc, oneshot};

    use crate::data_core::{Command, DataCore};
    use crate::parser::ParserValue;
    use crate::tokenizer::Token;

    #[test]
    fn test_responds_to_ping_command() {
        let (tx, rx) = oneshot::channel::<Vec<Token>>();
        let command = Command::new(
            Arc::new(vec![ParserValue::BulkString("PING".to_string())]),
            tx,
        );

        let (command_tx, command_rx) = mpsc::channel::<Command>(32);
        let data_core = DataCore::new(command_rx);
    }
}
