use tokio_postgres::{Config, Error, NoTls, AsyncMessage};
use log::error;
use futures::StreamExt;

use postgres_notify::notification_stream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = "host=localhost user=some password=dummy dbname=testify"
        .parse::<Config>()
        .unwrap();

    let (connection_handle, _client, mut notifications) = notification_stream(&config, NoTls, &["test_messages"]).await?;

    while let Some(notification) = notifications.next().await {
        dbg!(notification);
    }

    tokio::try_join!(connection_handle).unwrap();

    Ok(())
}
