use tokio_postgres::{Config, Error};

use postgres_notify::{just_dbg, notify_listen};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = "host=localhost user=some password=dummy dbname=testify"
        .parse::<Config>()
        .unwrap();
    notify_listen("127.0.0.1:5432", &config, just_dbg)
        .await
        .unwrap();

    Ok(())
}
