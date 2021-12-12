use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{StreamExt};
use serde::Serialize;
/**
 * how does this work?
 * firstly, we have some source of data
 * secondly, we have some websockets connection
 * we forward that data along
*/

use tracing::{event, instrument, Level};

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


//use futures_util::{future, pin_mut, stream::TryStreamExt};

type Tx = UnboundedSender<Message>;
type Am<T> = Arc<Mutex<T>>;
pub type ServerState = Am<HashMap<SocketAddr, Tx>>;

/**
 * This thread is always looping as fast as possible, this is inefficient
 * No longer! With the new changes this is almost costless in terms of compute.
 * Costs around 308kb of ram tho
 */
#[instrument(level="debug")]
async fn intermediary<T: Serialize + std::fmt::Debug>(rx: Receiver<T>, subs: ServerState, server_name: &'static str) {
    event!(Level::INFO, "Data pipeline successfully constructed for server {}", server_name);

    for x in rx.iter() {
        if let Ok(s) = serde_json::to_string(&x) {
            event!(Level::DEBUG, "New message {} received by datapipeline, successfully serialized, publishing.", s);
            let msg = Message::Text(s);
            tokio::spawn(publish_handler(subs.clone(), msg, server_name));
        } else {
            event!(Level::ERROR, "New message {:?} received by datapipeline, failure to serialize, not publishing, nonfatal.", &x);
        }
    }

    // we need to kill the channel?
}

#[instrument(level="debug")]
async fn publish_handler(recipients: ServerState, message: Message, server_name: &str) -> Result<()> {
    let tmp = recipients.lock().unwrap();
    for (_, sock) in tmp.iter() { // might be able to parallelize further using Rayon. might not be a good idea tho
        match sock.unbounded_send(message.clone()) {
            Ok(_) => {},
            Err(err) => {event!(Level::ERROR, "publish_handler on server {} encountered an error: {}", server_name, err);},
        }
    }

    Ok(())
}

#[instrument(level="debug")]
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

    loop {
        if let Ok((stream, addr)) = tcp_socket.accept().await {
            tokio::spawn(json_server_conn_handler(state.clone(), stream, addr, server_name));
        } else {
            event!(Level::ERROR, "TCP Listener for server {} tried to accept a connection and failed!", server_name);
        }
    }

    //while 

    //event!(Level::ERROR, "TCP Listener for server {} died!", server_name);
    //Ok(())
}

#[instrument(level="debug")]
async fn json_server_conn_handler(state: ServerState, raw_stream: TcpStream, addr: SocketAddr, server_name: &str) {
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect(&format!("[ERROR]: {}: error accepting connection from {}", server_name, addr));

    let (tx, rx) = unbounded();


    match state.lock() {
        Ok(mut x) => {
            x.insert(addr, tx);
            event!(Level::INFO, "{} spawned new connection at {}, {} conns outstanding", server_name, addr, x.len());
        },
        Err(_) => {event!(Level::ERROR, "Failed to get a lock on state!");},
    };

    let (outgoing, _) = ws_stream.split();

    let receiver = rx.map(Ok).forward(outgoing);

    match receiver.await {
        Ok(_) => {
            event!(Level::DEBUG, "Receiver exited with no error.");
        },
        Err(e) => {
            event!(Level::ERROR, "Receiver exited with error {}.", e);
        },
    };// unused result

    match state.lock() {
        Ok(mut x) => {
            x.remove(&addr);
            event!(Level::INFO, "{} dropped connection at {}, {} conns outstanding", server_name, addr, x.len());
        },
        Err(_) => {event!(Level::ERROR, "Failed to get a lock on state!");},
    };

}

#[instrument(level="info")]
pub async fn spinup<T: Serialize + Send + 'static + std::fmt::Debug, U: ToSocketAddrs + Send + 'static + std::fmt::Debug>(
    source: Receiver<T>,
    addr: U,
    name: &'static str
) -> ServerState {
    event!(Level::INFO, "Attempting to start serder {}", name);
    let state: ServerState = Arc::new(Mutex::new(HashMap::new()));
    task::spawn(listener(addr, state.clone(), name));
    task::spawn(intermediary(source, state.clone(), name));
    state
}

/*
 * TODO: Right now we run into the max number of connections we can have on one TCP socket before the code hits its limits
 * TODO: Up the system limits
 * TODO: Fix the Jaeger logging system - uses arbitrarily much RAM, really really fast. Logs need to be stored on a DB I think.
 * TODO: we might not even need Jaeger logging tbh, even though it's cool at this point. 
 * TODO: https://stackoverflow.com/questions/22090229/how-did-whatsapp-achieve-2-million-connections-per-server
 * TODO: How do we handle messages sent from the other side? Rn we don't do anything with them, and just error out if they disconnect. Good for now. 
 */

