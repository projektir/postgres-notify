use futures::channel::mpsc;

use tokio::net::TcpStream;
use tokio_postgres::{AsyncMessage, NoTls, Config, Client, Notification};
use futures::{future, stream, FutureExt, StreamExt, TryStreamExt};

#[derive(Debug)]
pub enum PsqlNotifyError {
    TcpStreamError(std::io::Error),
    TokioPostgresError(tokio_postgres::error::Error),
}

impl From<std::io::Error> for PsqlNotifyError {
    fn from(item: std::io::Error) -> Self {
        PsqlNotifyError::TcpStreamError(item)
    }
}

impl From<tokio_postgres::error::Error> for PsqlNotifyError {
    fn from(item: tokio_postgres::error::Error) -> Self {
        PsqlNotifyError::TokioPostgresError(item)
    }
}

pub async fn notify_listen<F>(socket_url: &str, config: &Config, mut notify_fn: F)
    -> Result<Client, PsqlNotifyError>
    where
        F: FnMut(Notification) + Send + 'static,
    {

    let (sender, receiver) = mpsc::unbounded();
    
    let socket = TcpStream::connect(socket_url).await?;
    let (client, mut connection) = config.connect_raw(socket, NoTls).await?;
    let stream = stream::poll_fn(move |context| connection.poll_message(context)).map_err(|e| panic!(e));
    let connection = stream.forward(sender).map(|r| r.unwrap());

    let conn_spawn = tokio::spawn(connection);

    client
        .batch_execute("LISTEN test_messages; ")
        .await
        .unwrap();

        let mut notifications = receiver
        .filter_map(|m| match m {
            AsyncMessage::Notification(n) => future::ready(Some(n)),
            AsyncMessage::Notice(err) => {
                println!("Notice: {:?}", err);
                future::ready(None)
            },
            _ => future::ready(None),
    });

    
    while let Some(notification) = notifications.next().await {
        notify_fn(notification);
    }

    tokio::try_join!(conn_spawn).unwrap();

    Ok(client)
}

pub fn just_dbg(notification: Notification) {
    dbg!(notification);    
}
