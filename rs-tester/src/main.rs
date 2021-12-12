use websocket::ClientBuilder;

fn main() {
    let mut client = ClientBuilder::new("ws:localhost:8080")
        .unwrap()
        .connect_insecure()
        .unwrap();
    
    for message in client.incoming_messages() {
        if let Ok(x) = message {
            println!("{:?}", x);
        }
    }

}