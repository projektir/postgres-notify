use log::error;
use tokio_postgres::{AsyncMessage, Config, Error, NoTls};

use postgres_notify::notify_listen_with_fn;

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

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
