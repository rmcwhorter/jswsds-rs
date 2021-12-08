use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};
use serde::{Serialize};
/**
 * how does this work?
 * firstly, we have some source of data
 * secondly, we have some websockets connection
 * we forward that data along
*/
use std::{
    marker::Send,
    collections::HashMap,
    net::SocketAddr,
    sync::{
        mpsc::{Receiver},
        Arc, Mutex
    },
};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task,
};
use tokio_tungstenite::tungstenite::protocol::Message;

use anyhow::{Result};

pub type Tx = UnboundedSender<Message>;
pub type Am<T> = Arc<Mutex<T>>;
pub type PubSubState = Am<HashMap<SocketAddr, Tx>>;

/**
 * !This thread is always looping as fast as possible, this is inefficient
 * No longer! With the new changes this is almost costless in terms of compute.
 * Costs around 308kb of ram tho
 */
async fn data_intermediary<T: Serialize>(rx: Receiver<T>, subs: PubSubState) {
    println!("[SUCCESS]: DATA PIPELINE CONSTRUCTED");

    for x in rx.iter() {
        if let Ok(s) = serde_json::to_string(&x) {
            //println!("New message outgoing: {}", &s);
            // we can probably parallelize this part some more

            let msg = Message::Text(s);
            //println!("[INTERMEDIARY]: Calling Ballista");
            tokio::spawn(ballista(subs.clone(), msg));
            //println!();
        }
    }

    // we need to kill the channel
}

async fn ballista(recipients: PubSubState, message: Message) {
    let tmp = recipients.lock().unwrap();
    //println!("[BALLISTA]: Sending message to {} subs", tmp.len());
    for (_, sock) in tmp.iter() {
        //println!("SENDING over unbounded sender");
        sock.unbounded_send(message.clone()).unwrap();
    }
}


async fn listener<K: ToSocketAddrs>(bind_addr: K, state: PubSubState) -> Result<()> {
    let tcp_socket = TcpListener::bind(bind_addr).await?;
    println!("[SUCCESS]: LISTENER BOUND TO {}", tcp_socket.local_addr().unwrap());
    
    while let Ok((stream, addr)) = tcp_socket.accept().await {
        println!("Spawning new conn!");
        tokio::spawn(json_server_conn_handler(state.clone(), stream, addr));
    }

    println!("listener death");
    Ok(())
}

pub async fn json_server_conn_handler(state: PubSubState, raw_stream: TcpStream, addr: SocketAddr) {
    //println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");

    let (tx, rx) = unbounded();
    state.lock().unwrap().insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    let receiver = rx.map(Ok).forward(outgoing);
    receiver.await;

    state.lock().unwrap().remove(&addr);
}

pub async fn launch_server<T: Serialize + Send + 'static, U: ToSocketAddrs + Send + 'static>(source: Receiver<T>, addr: U) {
    let state: PubSubState = Arc::new(Mutex::new(HashMap::new()));
    task::spawn(listener(addr, state.clone()));
    task::spawn(data_intermediary(source, state.clone()));
}