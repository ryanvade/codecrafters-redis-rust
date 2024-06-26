use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;

use chrono::{TimeDelta, Utc};
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
struct DataValue {
    parser_value: ParserValue,
    expiry_in_nanoseconds: Option<i64>,
}

impl DataValue {
    pub fn new(parser_value: ParserValue) -> DataValue {
        DataValue {
            parser_value,
            expiry_in_nanoseconds: None,
        }
    }

    pub fn set_expiry(self: &mut DataValue, milliseconds: i64) {
        let nano_seconds = Utc::now()
            .add(TimeDelta::milliseconds(milliseconds))
            .timestamp_nanos_opt()
            .unwrap();
        self.expiry_in_nanoseconds = Some(nano_seconds)
    }

    pub fn has_expired(self: &DataValue) -> bool {
        if self.expiry_in_nanoseconds.is_none() {
            return false;
        }
        let expiry_in_nanoseconds = self.expiry_in_nanoseconds.unwrap();
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        now > expiry_in_nanoseconds
    }
}

#[derive(Debug)]
pub struct DataCore {
    data_set: HashMap<String, DataValue>,
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

                    if !key.is_string() {
                        let response_value = ParserValue::NullBulkString;
                        return command
                            .response_channel
                            .send(response_value.to_tokens())
                            .unwrap();
                    }

                    let key = key
                        .to_string()
                        .expect("string parser value should be convertable to string");
                    let mut data_value = DataValue::new(value.clone());

                    if iter.peek().is_some_and(|pv| pv.is_string()) {
                        let _ = iter.next().unwrap().to_string().unwrap();
                        if iter.peek().is_some_and(|len| len.is_string()) {
                            let len = iter.next().unwrap().to_string().unwrap();
                            let len = len.parse::<i64>().expect("len string should be i64");
                            data_value.set_expiry(len)
                        }
                    }
                    self.data_set.insert(key, data_value);
                    let parser_value = ParserValue::SimpleString(String::from("OK"));
                    let response_tokens = parser_value.to_tokens();
                    command.response_channel.send(response_tokens).unwrap();
                }
                "get" => {
                    let mut iter = command.arguments.iter();
                    let _ = iter.next();
                    let key = iter.next().expect("get command should have a key");
                    if !key.is_string() {
                        let response_value = ParserValue::NullBulkString;
                        return command
                            .response_channel
                            .send(response_value.to_tokens())
                            .unwrap();
                    }

                    let key = key
                        .to_string()
                        .expect("string parser value should be convertable to a string");
                    let value = self.data_set.get(&key);
                    if value.is_none() {
                        let response_value = ParserValue::NullBulkString;
                        return command
                            .response_channel
                            .send(response_value.to_tokens())
                            .unwrap();
                    }
                    let value = value.unwrap();
                    let now = Utc::now().timestamp_nanos_opt().unwrap();
                    eprintln!("{:?} {:?}", value, now);
                    if value.has_expired() {
                        let _ = self.data_set.remove(&key);
                        let response_value = ParserValue::NullBulkString;
                        return command
                            .response_channel
                            .send(response_value.to_tokens())
                            .unwrap();
                    }

                    command
                        .response_channel
                        .send(value.parser_value.to_tokens())
                        .unwrap()
                }
                "command" => {
                    let parser_value = ParserValue::SimpleString(String::from(""));
                    let response = parser_value.to_tokens();
                    eprintln!("COMMAND response_tokens {:?}", response);
                    command.response_channel.send(response).unwrap();
                }
                _ => todo!(),
            }

            self.remove_expired_values()
        }
    }

    pub fn remove_expired_values(self: &mut DataCore) {
        eprintln!("Remove Expired Values");
        self.data_set.retain(|_, v| !v.has_expired())
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
