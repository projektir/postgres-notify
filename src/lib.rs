use futures::{future, stream, StreamExt};
use tokio_postgres::{AsyncMessage, Client, Config, NoTls};

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

pub async fn notify_listen<F>(
    execute_string: &str,
    config: &Config,
    mut notify_fn: F,
) -> Result<Client, PsqlNotifyError>
where
    F: FnMut(AsyncMessage) + Send + 'static,
{
    let (client, mut connection) = config.connect(NoTls).await?;
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
