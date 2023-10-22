use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::TcpListener};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        // tokio::spawn(async move {
        //     // split the socket into a reader handle and a writer handle
        //     // io::splitだとArc<Mutex<T>>が生成され、TcpStream::splitだと(ゼロコストの)参照が取得できる
        //     let (mut rd, mut wr) = socket.split();

        //     if io::copy(&mut rd, &mut wr).await.is_err() {
        //         eprintln!("failed to copy");
        //     }
        // });

        // manual copy
        tokio::spawn(async move {
            // heap buffer to store the read data and write it to other variant
            let mut buf = vec![0; 1024];

            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return , // EOF: when socket is closed
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            eprintln!("failed to write");
                            return;
                        }
                    }
                    Err(_) => {
                        eprintln!("failed to copy");
                        return;
                    }
                }
            }
        });
    }
}
