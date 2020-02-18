use tokio_postgres::{Config, Error, NoTls, AsyncMessage};
use log::error;

use postgres_notify::notify_listen_with_fn;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = "host=localhost user=some password=dummy dbname=testify"
        .parse::<Config>()
        .unwrap();
        notify_listen_with_fn(&"LISTEN test_messages;", &config, NoTls, just_dbg)
        .await
        .unwrap();

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
