use tokio_postgres::{Error, Config};

use postgres_notify::{notify_listen, just_dbg};

#[tokio::main]
async fn main() -> Result<(), Error> {

    let config = "host=localhost user=some password=dummy dbname=testify".parse::<Config>().unwrap();
    notify_listen("127.0.0.1:5432", &config, just_dbg).await.unwrap();

    Ok(())
}
