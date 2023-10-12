use std::{sync::{Arc, Mutex}, collections::HashMap};

use bytes::Bytes;
use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
/// mini-redis server
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening");

    // 複数スレッド(複数タスク)間で状態を共有するDBの定義
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // タスクごとに(スレッドセーフな)クローンを取得: タスク間で状態を共有するため
        let db = db.clone();

        // spawnで並行処理可能なタスクを生成: スレッド(並列)数に関わらず同時並行で処理される
        tokio::spawn(async move { // moveで変数の所有権をタスクに移動させる必要がある
            process(socket, db).await; // 所有権の移動に伴い、状態共有変数は事前に(対象全体を)クローンして別変数に格納すること！
        });
    }
}

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    // 各リクエストに対する処理を実行
    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                // 値の変更前に排他制御を実施
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                // 値を取得する前に排他制御を実施
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
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
