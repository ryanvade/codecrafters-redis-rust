use chrono::{TimeDelta, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::ops::Add;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

#[derive(Debug, PartialEq, Eq)]
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
    rx: Receiver<Command>,
    replication_role: ReplicationRole,
    connected_slaves: i64,
    master_replid: String,
    master_reploffset: i64,
    second_reploffset: i64,
    repl_backlog_active: i64,
    repl_backlog_size: i64,
    repl_backlog_first_byte_offset: i64,
    repl_backlog_histlen: i64,
    master_host: Option<String>,
    master_port: Option<u64>,
}

impl DataCore {
    pub fn new(
        rx: Receiver<Command>,
        replication_role: ReplicationRole,
        master_host: Option<String>,
        master_port: Option<u64>,
    ) -> DataCore {
        DataCore {
            data_set: HashMap::new(),
            rx,
            replication_role,
            connected_slaves: 0,
            master_replid: thread_rng()
                .sample_iter(&Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            master_reploffset: 0,
            second_reploffset: -1,
            repl_backlog_active: 0,
            repl_backlog_size: 1048576,
            repl_backlog_first_byte_offset: 0,
            repl_backlog_histlen: 0,
            master_host,
            master_port,
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
                "info" => {
                    let str = format!(
                        "# Replication\nrole:{}\nconnected_slaves:{}\nmaster_replid:{}\nmaster_repl_offset:{}\nsecond_repl_offset:{}\nrepl_backlog_active:{}\nrepl_backlog_size:{}\nrepl_backlog_first_byte_offset:{}\nrepl_backlog_histen:{}",
                        self.replication_role.to_string(),
                        self.connected_slaves,
                        self.master_replid,
                        self.master_reploffset,
                        self.second_reploffset,
                        self.repl_backlog_active,
                        self.repl_backlog_size,
                        self.repl_backlog_first_byte_offset,
                        self.repl_backlog_histlen
                    );
                    let response_value = ParserValue::BulkString(str);
                    return command
                        .response_channel
                        .send(response_value.to_tokens())
                        .unwrap();
                }
                "replconf" => {
                    let parser_value = ParserValue::SimpleString(String::from("OK"));
                    let response = parser_value.to_tokens();
                    eprintln!("REPLCONF Response {:?}", response);
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

    pub async fn initialize_slaves(
        self: &mut DataCore,
        slave_port: u64,
    ) -> anyhow::Result<(), Box<dyn Error>> {
        let ping = ParserValue::Array(vec![ParserValue::SimpleString("PING".to_string())]);
        let master_connection_string = format!(
            "{}:{}",
            self.master_host.as_ref().unwrap(),
            self.master_port.unwrap()
        );
        eprintln!("Master connection string: {:?}", master_connection_string);

        let mut stream = TcpStream::connect(master_connection_string).await?;
        stream.writable().await?;

        let ping = tokenizer::serialize_tokens(&ping.to_tokens())
            .expect("ping parser value array should be serializable");
        stream.write_all(ping.into_bytes().as_ref()).await?;
        stream.flush().await?;

        let mut buff = [0; 8];
        loop {
            let response = stream.read(&mut buff).await?;
            eprintln!("Ping Response Length: {:?}", response);
            if response == 7 {
                break;
            }
        }
        eprintln!(
            "Initialize Slaves Ping Response: {:?}",
            String::from_utf8(buff.to_vec())
        );

        let listening_port = ParserValue::Array(vec![
            ParserValue::SimpleString("REPLCONF".to_string()),
            ParserValue::SimpleString("listening-port".to_string()),
            ParserValue::SimpleString(slave_port.to_string()),
        ]);
        let listening_port = tokenizer::serialize_tokens(&listening_port.to_tokens())
            .expect("listening-port parser value array should be serializable");
        stream
            .write_all(listening_port.into_bytes().as_ref())
            .await?;
        stream.flush().await?;

        let mut buff = [0; 8];
        loop {
            let response = stream.read(&mut buff).await?;
            eprintln!("Listening Port Response Length: {:?}", response);
            if response == 5 {
                break;
            }
        }
        eprintln!(
            "Initialize Slave listening-port Response: {:?}",
            String::from_utf8(buff.to_vec())
        );

        let capabilities = ParserValue::Array(vec![
            ParserValue::SimpleString("REPLCONF".to_string()),
            ParserValue::SimpleString("capa".to_string()),
            ParserValue::SimpleString("psync2".to_string()),
        ]);
        let capabilities = tokenizer::serialize_tokens(&capabilities.to_tokens())
            .expect("capabilities parser value array should be serializable");
        stream.write_all(capabilities.into_bytes().as_ref()).await?;
        stream.flush().await?;

        let mut buff = [0; 8];
        loop {
            let response = stream.read(&mut buff).await?;
            eprintln!("Capa Response Length: {:?}", response);
            if response == 5 {
                break;
            }
        }
        eprintln!(
            "Initialize capabilities Response: {:?}",
            String::from_utf8(buff.to_vec())
        );

        let psync = ParserValue::Array(vec![
            ParserValue::SimpleString("PSYNC".to_string()),
            ParserValue::SimpleString("?".to_string()),
            ParserValue::SimpleString("-1".to_string()),
        ]);
        let psync = tokenizer::serialize_tokens(&psync.to_tokens())
            .expect("psync parser value array should be serializable");
        stream.write_all(psync.into_bytes().as_ref()).await?;
        stream.flush().await?;

        let mut buff = [0; 58];
        loop {
            let response = stream.read(&mut buff).await?;
            eprintln!("PSYNC Response Length: {:?}", response);
            if response >= 56 {
                break;
            }
        }
        eprintln!(
            "Initialize capabilities Response: {:?}",
            String::from_utf8(buff.to_vec())
        );

        let full_resync_response =
            String::from_utf8(buff.to_vec()).expect("full resync response should be stringable");
        let full_resync_response = full_resync_response.splitn(3, ' ').collect::<Vec<_>>();
        let replica_id = full_resync_response
            .get(1)
            .expect("full resync response should have a replica_id");
        eprintln!("Replica Id: {:?}", replica_id);

        Ok(())
    }

    pub fn is_slave(self: &DataCore) -> bool {
        self.replication_role == ReplicationRole::Slave
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{mpsc, oneshot};

    use crate::data_core::{Command, DataCore, ReplicationRole};
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
        let data_core = DataCore::new(command_rx, ReplicationRole::Master, None, None);
    }
}
