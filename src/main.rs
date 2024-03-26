// Uncomment this block to pass the first stage
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("0.0.0.0:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        process_request(socket).await;
    }

    Ok(())
}

async fn process_request<T>(socket: T) {
    println!("accepted new connection");
}
