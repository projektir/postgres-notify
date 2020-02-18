use futures::{future, stream, StreamExt};
use tokio_postgres::{AsyncMessage, Client, Config, Socket};
use tokio_postgres::tls::MakeTlsConnect;

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

pub async fn notify_listen<T, F>(
    execute_string: &str,
    config: &Config,
    tls: T,
    mut notify_fn: F,
) -> Result<Client, PsqlNotifyError>
where
    T: MakeTlsConnect<Socket> + 'static + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    F: FnMut(AsyncMessage) + Send + 'static,
{
    let (client, mut connection) = config.connect(tls).await?;
    let connection = stream::poll_fn(move |cx| connection.poll_message(cx));

    let conn_spawn = tokio::spawn(connection.for_each(move |r| {
        match r {
            Ok(m) => notify_fn(m),
            Err(e) => eprintln!("postgres connection error: {}", e),
        }

        future::ready(())
    }));

    client.batch_execute(execute_string).await.unwrap();

    tokio::try_join!(conn_spawn).unwrap();

    Ok(client)
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
            eprintln!("new AsyncMessage");
        }
    };
}
