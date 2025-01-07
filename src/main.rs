use clap::Parser;
use tokio::net::TcpListener;

use redis_starter_rust::data_core::ReplicationRole;
use redis_starter_rust::server;

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

    let addr = format!("0.0.0.0:{}", args.port);

    let listener = TcpListener::bind(addr)
        .await
        .expect("cannot listen on port 6379");

    server::serve(listener, replication_role, master_host, master_port)
        .await
        .expect("server error");
}
