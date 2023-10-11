use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
/// mini-redis server
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // spawnで並行処理可能なタスクを生成: スレッド(並列)数に関わらず同時並行で処理される
        tokio::spawn(async move { // moveで変数の所有権をタスクに移動させる必要がある
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    use mini_redis::Command::{self, Get, Set};
    use std::collections::HashMap;

    // ソケット(クライアントからの接続経路)単位でDBを定義
    let mut db = HashMap::new();

    let mut connection = Connection::new(socket);

    // 各リクエストに対する処理を実行
    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                // store value as bytes array
                db.insert(cmd.key().to_string(), cmd.value().to_vec());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => {
                panic!("unimplemented: {:?}", cmd)
            }
        };

        connection.write_frame(&response).await.unwrap();
    }
}
