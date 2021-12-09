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

use anyhow::{anyhow, Result};

use tracing::{span, event, instrument, Level};

type Tx = UnboundedSender<Message>;
type Am<T> = Arc<Mutex<T>>;
pub type ServerState = Am<HashMap<SocketAddr, Tx>>;

/**
 * This thread is always looping as fast as possible, this is inefficient
 * No longer! With the new changes this is almost costless in terms of compute.
 * Costs around 308kb of ram tho
 */
#[instrument(level="info")]
async fn intermediary<T: Serialize + std::fmt::Debug>(rx: Receiver<T>, subs: ServerState, server_name: &str) {
    event!(Level::INFO, "Data pipeline successfully constructed for server {}", server_name);

    for x in rx.iter() {
        if let Ok(s) = serde_json::to_string(&x) {
            event!(Level::DEBUG, "New message {} received by datapipeline, successfully serialized, publishing.", s);
            let msg = Message::Text(s);
            tokio::spawn(publish_handler(subs.clone(), msg));
        } else {
            event!(Level::ERROR, "New message {:?} received by datapipeline, failure to serialize, not publishing, nonfatal.", &x);
        }
    }

    // we need to kill the channel?
}

#[instrument(level="debug")]
async fn publish_handler(recipients: ServerState, message: Message) -> Result<()> {
    //let tmp = recipients.lock().unwrap(); // got a poison error on this thing when killing 200 conns at once

    /*let tmp = match  {
        Ok(mut x) => {x},
        Err(_) => {event!(Level::ERROR, "Failed to get a lock on state!");},
    };*/

    let tmp = recipients.lock().unwrap();

    for (_, sock) in tmp.iter() { // might be able to parallelize further using Rayon. might not be a good idea tho
        if let Ok(_) = sock.unbounded_send(message.clone()) {
            event!(Level::TRACE, "Message successfully sent");
        } else {
            event!(Level::ERROR, "Message failed to send");
        }
    }

    Ok(())
}

#[instrument(level="info")]
async fn listener<K: ToSocketAddrs + std::fmt::Debug>(bind_addr: K, state: ServerState, server_name: &'static str) -> Result<()> {
    
    let tcp_socket = match TcpListener::bind(bind_addr).await {
        Ok(x) => {
            event!(Level::INFO, "TCP Listener for server {} bound to {}", server_name, x.local_addr().unwrap());
            x
        },
        Err(x) => {
            event!(Level::ERROR, "TCP Listener for server {} failed to bind!", server_name);
            return Err(anyhow!(x));
        }
    };

    while let Ok((stream, addr)) = tcp_socket.accept().await {
        event!(Level::INFO, "Server {} is spawning new connection at {}", server_name, addr);
        tokio::spawn(json_server_conn_handler(state.clone(), stream, addr, server_name));
    }

    println!("listener death");
    Ok(())
}

#[instrument(level="debug")]
async fn json_server_conn_handler(state: ServerState, raw_stream: TcpStream, addr: SocketAddr, server_name: &str) {
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect(&format!("[ERROR]: {}: error accepting connection from {}", server_name, addr));

    let (tx, rx) = unbounded();

    match state.lock() {
        Ok(mut x) => {x.insert(addr, tx);},
        Err(_) => {event!(Level::ERROR, "Failed to get a lock on state!");},
    };

    let (outgoing, _) = ws_stream.split();

    let receiver = rx.map(Ok).forward(outgoing);
    match receiver.await {
        Ok(_) => {
            event!(Level::DEBUG, "Receiver exited with no error.");
        },
        Err(e) => {
            event!(Level::DEBUG, "Receiver exited with error {}.", e);
        },
    };// unused result

    state.lock().unwrap().remove(&addr);
}

#[instrument(level="info")]
pub async fn spinup<T: Serialize + Send + 'static + std::fmt::Debug, U: ToSocketAddrs + Send + 'static + std::fmt::Debug>(
    source: Receiver<T>,
    addr: U,
    name: &'static str
) -> ServerState {
    let state: ServerState = Arc::new(Mutex::new(HashMap::new()));
    task::spawn(listener(addr, state.clone(), name));
    task::spawn(intermediary(source, state.clone(), name));
    state
}