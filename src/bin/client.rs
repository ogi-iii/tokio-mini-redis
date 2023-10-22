use bytes::Bytes;
use mini_redis::{client, Result};
use tokio::sync::{mpsc, oneshot::{Sender, self}};

#[tokio::main]
async fn main() {
    // multi-producer, single-consumer channel: 複数のリクエストを受け取るため
    let (tx, mut rx) = mpsc::channel(32);
    // mpscなので、送信チャネルは複製可能
    let tx2 = tx.clone();

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // receiver側でコマンドを受け取り、clientに順次渡す
        while let Some(cmd) = rx.recv().await {
            use Command::*;
            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res); // リクエスト側にチャネル経由で結果を返す: 他の送信者がいないためawaitは不要
                },
                Set { key, value, resp } => {
                    let res = client.set(&key, value).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

    let t1 = tokio::spawn(async move {
        // single-producer, single-consumer channel: clientからのレスポンスを自身(リクエスト送信者)のみが受け取るため
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx, // 送信側を渡す: 結果を返してもらうため
        };
        tx.send(cmd).await.unwrap();

        // チャネルに結果が返されるまで待機
        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });

    let t2 = tokio::spawn(async move {
        // single-producer, single-consumer channel
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            value: "bar".into(),
            resp: resp_tx,
        };
        tx2.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });

    // 各処理が完了するまで待機する
    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>
    },
    Set {
        key: String,
        value: Bytes,
        resp: Responder<()>
    }
}

// チャネル送信側の独自型
type Responder<T> = Sender<Result<T>>;
