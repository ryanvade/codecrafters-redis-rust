use crate::client_connection::ClientConnection;
use crate::data_core::{DataCore, ReplicationRole};
use crate::parser::ParserValue;
use crate::tokenizer;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ReplicationSettings {
    pub replication_role: ReplicationRole,
    pub connected_slaves: i64,
    pub master_replid: String,
    pub master_reploffset: i64,
    pub second_reploffset: i64,
    pub repl_backlog_active: i64,
    pub repl_backlog_size: i64,
    pub repl_backlog_first_byte_offset: i64,
    pub repl_backlog_histlen: i64,
    pub master_host: Option<String>,
    pub master_port: Option<u64>,
}

#[derive(Debug)]
pub struct Server {
    tcp_listener: TcpListener,
    data_core: Arc<Mutex<DataCore>>,
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

impl Server {
    pub fn new(
        listener: TcpListener,
        replication_role: ReplicationRole,
        master_host: Option<String>,
        master_port: Option<u64>,
    ) -> Server {
        Server {
            tcp_listener: listener,
            replication_role: replication_role.clone(),
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
            master_host: master_host.clone(),
            master_port,
            data_core: Arc::new(Mutex::new(DataCore::new())),
        }
    }

    pub fn is_secondary(&self) -> bool {
        self.replication_role == ReplicationRole::Slave
    }

    pub fn replication_settings(&self) -> ReplicationSettings {
        ReplicationSettings {
            replication_role: self.replication_role.clone(),
            connected_slaves: self.connected_slaves,
            master_replid: self.master_replid.clone(),
            master_reploffset: self.master_reploffset,
            second_reploffset: self.second_reploffset,
            repl_backlog_active: self.repl_backlog_active,
            repl_backlog_size: self.repl_backlog_size,
            repl_backlog_first_byte_offset: self.repl_backlog_first_byte_offset,
            repl_backlog_histlen: self.repl_backlog_histlen,
            master_host: self.master_host.clone(),
            master_port: self.master_port,
        }
    }

    pub async fn connect_to_primary(&mut self) -> anyhow::Result<(), Box<dyn Error>> {
        let self_port = self.tcp_listener.local_addr()?.port();
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
            ParserValue::SimpleString(self_port.to_string()),
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
            ParserValue::BulkString("PSYNC".to_string()),
            ParserValue::BulkString("?".to_string()),
            ParserValue::BulkString("-1".to_string()),
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
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let role = self.replication_role.to_string();
        let host = self.tcp_listener.local_addr().unwrap().ip().to_string();

        write!(f, "Server type: {}, Server Address: {}", role, host)
    }
}

pub async fn serve(
    listener: TcpListener,
    replication_role: ReplicationRole,
    master_host: Option<String>,
    master_port: Option<u64>,
) -> anyhow::Result<()> {
    let mut server = Server::new(listener, replication_role, master_host, master_port);

    if server.is_secondary() {
        server
            .connect_to_primary()
            .await
            .expect("Couldn't connect to the primary server");
    }

    loop {
        let (peer_tcp_stream, peer_addr) = server
            .tcp_listener
            .accept()
            .await
            .expect("cannot accept connections");

        let mut client_connection = ClientConnection::new(peer_tcp_stream, peer_addr);

        let replication_settings = server.replication_settings();
        let data_core = Arc::clone(&server.data_core);
        tokio::spawn(async move {
            client_connection
                .handle_requests(data_core, replication_settings)
                .await;
        });
    }

    Ok(())
}
