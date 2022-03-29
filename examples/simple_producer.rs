use tracing::info;
use rocketmq::producer::{Producer, ProducerOptions, SendStatus};
use rocketmq::Message;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let mut options = ProducerOptions::default();
    options.set_name_server(vec!["10.248.129.188:9876".to_string()]);
    info!("options: {:?}", options);

    let producer = Producer::with_options(options).unwrap();
    producer.start();

    let msg = Message::new(
        "TEST_TOPIC".to_string(),
        String::new(),
        String::new(),
        0,
        b"test".to_vec(),
        false,
    );

    let ret = producer.send(msg).await;
    info!("res {:?}", ret);

    // assert_eq!(ret.status, SendStatus::Ok);
}
