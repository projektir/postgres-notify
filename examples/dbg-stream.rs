use tokio_postgres::{Config, Error, NoTls, AsyncMessage};
use log::error;
use futures::stream::StreamExt;

use postgres_notify::notify_stream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    
    let config = "host=localhost user=elevate_user password=dummy dbname=testify"
        .parse::<Config>()
        .unwrap();

    let (connection_handle, mut messages, _client) = notify_stream(&"LISTEN test_messages;", &config, NoTls).await?;

    while let Some(message) = messages.next().await {
        just_dbg(message);
    }

    tokio::try_join!(connection_handle).unwrap();

    Ok(())
}

pub fn just_dbg(message: AsyncMessage) {
    match message {
        AsyncMessage::Notification(n) => {
            dbg!(n);
        }
        AsyncMessage::Notice(err) => {
            dbg!(err);
        }
        _ => {
            error!("new AsyncMessage on nonexhaustive type");
        }
    };
}
