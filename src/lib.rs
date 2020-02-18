use futures::{channel::mpsc, future, stream, FutureExt, StreamExt, TryStreamExt};
use log::error;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_postgres::{
    tls::MakeTlsConnect, types::ToSql, AsyncMessage, Client, Config, Error, Notification, Socket,
};

/// Convenience function for long-run processing of Postgres notifications.
/// Connects to the Postgres database with the provided `Config`, spawns a tokio runtime to listen for messages,
/// and calls `batch_execute` on the provided `execute_string`.
/// The spawned stream will run `notify_fn` on each `AsyncMessage`.
///
/// There is no validation of any kind on `execute_string`. It is implied here that simply "LISTEN test_messages;" or
/// similar will be sent, other execution is at your own risk.
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

pub async fn notification_stream<T>(
    config: &Config,
    tls: T,
    channels: &[&str],
) -> Result<
    (
        JoinHandle<()>,
        Client,
        impl futures::Stream<Item = Notification>,
    ),
    Error,
>
where
    T: MakeTlsConnect<Socket> + 'static + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
{
    let (sender, receiver) = mpsc::unbounded();
    let listen_channels: Arc<[String]> = channels.iter().map(|x| x.to_string()).collect();

    let (client, mut connection) = config.connect(tls).await?;
    let connection = stream::poll_fn(move |cx| connection.poll_message(cx));

    let conn_spawn = tokio::spawn(connection.for_each(move |r| {
        match r {
            Ok(m) => {
                sender.unbounded_send(m).expect("channel error");
            }
            Err(e) => error!("postgres connection error: {}", e),
        }

        future::ready(())
    }));

    client
        .batch_execute(
            &listen_channels
                .iter()
                .fold(String::new(), |mut acc, channel| {
                    acc.push_str("LISTEN ");
                    acc.push_str(&channel); // TODO: sanitize this
                    acc.push_str(";");
                    acc
                }),
        )
        .await?;

    let notifications = receiver
        .filter_map(|m| match m {
            AsyncMessage::Notification(n) => future::ready(Some(n)),
            AsyncMessage::Notice(err) => {
                println!("Notice: {:?}", err);
                future::ready(None)
            }
            _ => future::ready(None),
        })
        .filter(move |n| future::ready(listen_channels.iter().any(|c| n.channel() == c.as_str())));

    Ok((conn_spawn, client, notifications))
}
