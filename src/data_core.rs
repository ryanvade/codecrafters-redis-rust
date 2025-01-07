use chrono::{TimeDelta, Utc};
use std::collections::HashMap;
use std::fmt;
use std::ops::Add;
use std::sync::Arc;

use crate::parser::ParserValue;
use crate::server::ReplicationSettings;
use crate::tokenizer::Token;

#[derive(Debug)]
pub struct Command {
    pub arguments: Arc<Vec<ParserValue>>,
    pub replication_settings: ReplicationSettings,
}

impl Command {
    pub fn new(
        arguments: Arc<Vec<ParserValue>>,
        replication_settings: ReplicationSettings,
    ) -> Command {
        Command {
            arguments,
            replication_settings,
        }
    }

    pub fn is_psync(&self) -> bool {
        let first = self
            .arguments
            .first()
            .expect("arguments should have at least one argument");

        first
            .to_string()
            .expect("first should always be a string")
            .to_lowercase()
            .as_str()
            == "psync"
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReplicationRole {
    Master,
    Slave,
}

impl fmt::Display for ReplicationRole {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplicationRole::Master => write!(f, "master"),
            ReplicationRole::Slave => write!(f, "slave"),
        }
    }
}

#[derive(Debug)]
pub struct DataCore {
    data_set: HashMap<String, DataValue>,
}

impl Default for DataCore {
    fn default() -> Self {
        Self::new()
    }
}

impl DataCore {
    pub fn new() -> DataCore {
        DataCore {
            data_set: HashMap::new(),
        }
    }

    pub async fn process_command(self: &mut DataCore, command: Command) -> Vec<Token> {
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
                return response;
            }
            "echo" => {
                let mut tokens: Vec<Token> = Vec::new();
                let mut iter = command.arguments.iter();
                let _ = iter.next();
                // TODO: how to handle multiple strings passed to echo?
                for echo_str_token in iter {
                    if let Some(echo_str) = echo_str_token.to_string() {
                        let parser_value = ParserValue::BulkString(echo_str);
                        let mut response_tokens = parser_value.to_tokens();
                        tokens.append(&mut response_tokens);
                    }
                }
                return tokens;
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
                    return response_value.to_tokens();
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
                return response_tokens;
            }
            "get" => {
                let mut iter = command.arguments.iter();
                let _ = iter.next();
                let key = iter.next().expect("get command should have a key");
                if !key.is_string() {
                    let response_value = ParserValue::NullBulkString;
                    return response_value.to_tokens();
                }

                let key = key
                    .to_string()
                    .expect("string parser value should be convertable to a string");
                let value = self.data_set.get(&key);
                if value.is_none() {
                    let response_value = ParserValue::NullBulkString;
                    return response_value.to_tokens();
                }
                let value = value.unwrap();
                let now = Utc::now().timestamp_nanos_opt().unwrap();
                eprintln!("{:?} {:?}", value, now);
                if value.has_expired() {
                    let _ = self.data_set.remove(&key);
                    let response_value = ParserValue::NullBulkString;
                    return response_value.to_tokens();
                }

                return value.parser_value.to_tokens();
            }
            "command" => {
                let parser_value = ParserValue::SimpleString(String::from(""));
                let response = parser_value.to_tokens();
                eprintln!("COMMAND response_tokens {:?}", response);
                return response;
            }
            "info" => {
                let str = format!(
                    "# Replication\nrole:{}\nconnected_slaves:{}\nmaster_replid:{}\nmaster_repl_offset:{}\nsecond_repl_offset:{}\nrepl_backlog_active:{}\nrepl_backlog_size:{}\nrepl_backlog_first_byte_offset:{}\nrepl_backlog_histen:{}",
                    command.replication_settings.replication_role,
                    command.replication_settings.connected_slaves,
                    command.replication_settings.master_replid,
                    command.replication_settings.master_reploffset,
                    command.replication_settings.second_reploffset,
                    command.replication_settings.repl_backlog_active,
                    command.replication_settings.repl_backlog_size,
                    command.replication_settings.repl_backlog_first_byte_offset,
                    command.replication_settings.repl_backlog_histlen
                );
                let response_value = ParserValue::BulkString(str);
                return response_value.to_tokens();
            }
            "replconf" => {
                let parser_value = ParserValue::SimpleString(String::from("OK"));
                let response = parser_value.to_tokens();
                eprintln!("REPLCONF Response {:?}", response);
                return response;
            }
            "psync" => {
                let parser_value = ParserValue::SimpleString(String::from(
                    "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
                ));
                let response = parser_value.to_tokens();
                eprintln!("PSYNC Response {:?}", response);
                return response;
            }
            _ => todo!(),
        }

        self.remove_expired_values();

        return ParserValue::NullBulkString.to_tokens();
    }

    pub fn remove_expired_values(self: &mut DataCore) {
        eprintln!("Remove Expired Values");
        self.data_set.retain(|_, v| !v.has_expired())
    }

    pub fn to_rdb_bytes(self: &DataCore) -> Vec<u8> {
        // TODO: Generate actual RDB File
        let empty = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("should be able to represent hex");
        let len = empty.len();
        let resp_str = format!("${}\r\n", len);
        let mut resp = resp_str.as_bytes().to_vec();
        resp.append(&mut empty.as_slice().to_vec());
        resp
    }
}
