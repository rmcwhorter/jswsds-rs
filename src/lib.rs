use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{StreamExt};
use serde::Serialize;
/**
 * how does this work?
 * firstly, we have some source of data
 * secondly, we have some websockets connection
 * we forward that data along
*/
use std::{
    collections::HashMap,
    marker::Send,
    net::SocketAddr,
    sync::{mpsc::Receiver, Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task,
};
use tokio_tungstenite::tungstenite::protocol::Message;

use anyhow::Result;

type Tx = UnboundedSender<Message>;
type Am<T> = Arc<Mutex<T>>;
pub type ServerState = Am<HashMap<SocketAddr, Tx>>;

/**
 * !This thread is always looping as fast as possible, this is inefficient
 * No longer! With the new changes this is almost costless in terms of compute.
 * Costs around 308kb of ram tho
 */
async fn intermediary<T: Serialize>(rx: Receiver<T>, subs: ServerState, server_name: &str) {
    println!("[SUCCESS]: {}: DATA PIPELINE CONSTRUCTED", server_name);

    for x in rx.iter() {
        if let Ok(s) = serde_json::to_string(&x) {
            let msg = Message::Text(s);
            tokio::spawn(ballista(subs.clone(), msg));
        }
    }

    // we need to kill the channel?
}

async fn ballista(recipients: ServerState, message: Message) {
    let tmp = recipients.lock().unwrap(); // got a poison error on this thing when killing 200 conns at once

    for (_, sock) in tmp.iter() { // might be able to parallelize further using Rayon. might not be a good idea tho
        sock.unbounded_send(message.clone());//.unwrap(); // unused result
    }
}

async fn listener<K: ToSocketAddrs>(bind_addr: K, state: ServerState, server_name: &'static str) -> Result<()> {
    let tcp_socket = TcpListener::bind(bind_addr).await?;
    println!(
        "[SUCCESS]: {}: LISTENER BOUND TO {}",
        server_name,
        tcp_socket.local_addr().unwrap()
    );

    while let Ok((stream, addr)) = tcp_socket.accept().await {
        println!("Spawning new conn!");
        tokio::spawn(json_server_conn_handler(state.clone(), stream, addr, server_name));
    }

    println!("listener death");
    Ok(())
}

async fn json_server_conn_handler(state: ServerState, raw_stream: TcpStream, addr: SocketAddr, server_name: &str) {
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect(&format!("[ERROR]: {}: error accepting connection from {}", server_name, addr));

    let (tx, rx) = unbounded();
    state.lock().unwrap().insert(addr, tx);

    let (outgoing, _) = ws_stream.split();

    let receiver = rx.map(Ok).forward(outgoing);
    receiver.await;// unused result

    state.lock().unwrap().remove(&addr);
}

pub async fn launch_server<T: Serialize + Send + 'static, U: ToSocketAddrs + Send + 'static>(
    source: Receiver<T>,
    addr: U,
    name: &'static str
) -> ServerState {
    let state: ServerState = Arc::new(Mutex::new(HashMap::new()));
    task::spawn(listener(addr, state.clone(), name));
    task::spawn(intermediary(source, state.clone(), name));
    state
}
