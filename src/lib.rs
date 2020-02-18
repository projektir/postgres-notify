use log::error;
use tokio::task::JoinHandle;
use futures::{
    stream::BoxStream,
    channel::mpsc, future, stream, StreamExt
};
use tokio_postgres::{
    tls::MakeTlsConnect, AsyncMessage, Client, Config, Error, Socket,
};

/// Convenience function for long-run processing of Postgres notifications.
/// Connects to the Postgres database with the provided `Config`, spawns a tokio runtime to listen for messages,
/// and calls `batch_execute` on the provided `execute_string`.
/// The spawned stream will run `notify_fn` on each `AsyncMessage`.
///
/// There is no validation of any kind on `execute_string`: this is the same as calling `bathc_execute` directly.
/// It is implied here that simply "LISTEN test_messages;" or similar will be sent, other execution is at your own risk.
pub async fn notify_listen_with_fn<T, F>(
    execute_string: &str,
    config: &Config,
    tls: T,
    mut notify_fn: F,
) -> Result<Client, Error>
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
            Err(e) => error!("postgres connection error: {}", e),
        }

        future::ready(())
    }));

    client.batch_execute(execute_string).await?;

    match tokio::try_join!(conn_spawn) {
        Err(err) => {
            error!("failed to join on connection; error = {}", err);
        }
        _ => {}
    }

    Ok(client)
}

/// Convenience function for long-run processing of Postgres notifications.
/// Connects to the Postgres database with the provided `Config`, spawns a tokio runtime to listen for messages,
/// and calls `batch_execute` on the provided `execute_string`.
/// Returns the spawned stream of `AsyncMessage`s.
///
/// There is no validation of any kind on `execute_string`: this is the same as calling `bathc_execute` directly.
/// It is implied here that simply "LISTEN test_messages;" or similar will be sent, other execution is at your own risk.
pub async fn notify_stream<T>(
    execute_string: &str,
    config: &Config,
    tls: T,
) -> Result<
    (
        JoinHandle<()>,
        BoxStream<'static, AsyncMessage>,
        Client,
    ),
    Error,
>
where
    T: MakeTlsConnect<Socket> + 'static + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
{
    let (sender, receiver) = mpsc::unbounded();

    let (client, mut connection) = config.connect(tls).await?;
    let connection = stream::poll_fn(move |cx| connection.poll_message(cx));
    
    let conn_spawn = tokio::spawn(connection.for_each(move |result| {
        match result {
            Ok(message) => {
                match sender.unbounded_send(message) {
                    Err(err) => {
                        error!("channel send error: {}", err);
                    },
                    _ => {}
                };
            },
            Err(err) => error!("postgres connection error: {}", err),
        }

        future::ready(())
    }));

    client.batch_execute(execute_string).await?;

    Ok((conn_spawn, receiver.boxed(), client))
}
