//! Message sessions against the Go fixture, as the edge drives them on Link.
use claviron::orbit::link::application::ExchangeBody;
use claviron::orbit::link::application::client::{LentRead, LentWrite};
use claviron::orbit::link::application::{
    ApplicationClient, Exchange, ExchangeReply, HttpVersion, Message,
    MessageReader, write_message,
};
use tokio::io::AsyncWriteExt;

use super::{App, Result, frame};

fn request(
    path: &str,
    version: HttpVersion,
) -> Exchange {
    Exchange { upgrade: true, version, ..frame(path) }
}

async fn open(
    client: &ApplicationClient,
    path: &str,
    version: HttpVersion,
) -> Result<(ExchangeReply, LentWrite, LentRead)> {
    let pending = client.send(
        "app",
        claviron::orbit::link::application::Exchange {
            body: ExchangeBody::Stream,
            ..request(path, version)
        },
    )?;
    let (input, output, reply, _) = pending.split();
    Ok((reply.head().await?, input, output))
}

fn header<'a>(
    head: &'a ExchangeReply,
    name: &[u8],
) -> Option<&'a [u8]> {
    head.headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_slice())
}

pub(super) async fn run(
    client: &ApplicationClient,
    app: &mut App,
) -> Result<()> {
    // The application decides before the client is answered.
    let mut refused = request("/session-reject", HttpVersion::Http11);
    refused.headers.push((b"x-token".to_vec(), b"denied".to_vec()));
    let (_, _, reply, _) = client
        .send(
            "app",
            claviron::orbit::link::application::Exchange {
                body: ExchangeBody::Stream,
                ..refused
            },
        )?
        .split();
    let head = reply.head().await?;
    assert_eq!(head.status, 403);
    assert_eq!(header(&head, b"x-refused"), Some(&b"denied"[..]));
    let (head, ..) =
        open(client, "/session-silent", HttpVersion::Http11).await?;
    assert_eq!(head.status, 501);
    let (head, ..) =
        open(client, "/session-panic", HttpVersion::Http11).await?;
    assert_eq!(head.status, 500);

    // Both protocols reach the same messages, and a peer's close is answered.
    for (version, kind) in [
        (HttpVersion::Http11, &b"websocket"[..]),
        (HttpVersion::Http3, &b"webtransport"[..]),
    ] {
        let (head, mut input, mut output) =
            open(client, "/session-echo", version).await?;
        assert_eq!(head.status, 101);
        assert_eq!(header(&head, b"x-accepted"), Some(kind));
        let mut records = MessageReader::new();
        for message in
            [Message::Text("hello".into()), Message::Binary(vec![0, 255, 7])]
        {
            write_message(&mut input, &message).await?;
            assert_eq!(records.read(&mut output).await?, Some(message));
        }
        let close = Message::Close { code: 4001, reason: "done".into() };
        write_message(&mut input, &close).await?;
        assert_eq!(records.read(&mut output).await?, Some(close));
        assert_eq!(records.read(&mut output).await?, None);
        app.expect("SESSION_CLOSE 4001 done").await?;
    }

    // A client gone without a close is not answered on the wire.
    let (_, mut input, mut output) =
        open(client, "/session-echo", HttpVersion::Http11).await?;
    let gone = Message::Close { code: 1006, reason: String::new() };
    write_message(&mut input, &gone).await?;
    let mut records = MessageReader::new();
    assert_eq!(records.read(&mut output).await?, None);
    app.expect("SESSION_CLOSE 1006 ").await?;

    // The direction's end without a close reaches the handler as EOF.
    let (_, mut input, _output) =
        open(client, "/session-echo", HttpVersion::Http11).await?;
    input.shutdown().await?;
    app.expect("SESSION_END").await?;

    // An application close, and an end without one.
    let (head, _input, mut output) =
        open(client, "/session-close", HttpVersion::Http11).await?;
    assert_eq!(head.status, 101);
    let mut records = MessageReader::new();
    assert_eq!(
        records.read(&mut output).await?,
        Some(Message::Close { code: 4000, reason: "bye".into() })
    );
    assert_eq!(records.read(&mut output).await?, None);
    let (head, _input, mut output) =
        open(client, "/session-end", HttpVersion::Http11).await?;
    assert_eq!(head.status, 101);
    assert_eq!(MessageReader::new().read(&mut output).await?, None);

    // A message over the application's limit ends the session; the control
    // allowance is the reader's floor, so this one is over it too.
    let (_, mut input, _output) =
        open(client, "/session-echo", HttpVersion::Http11).await?;
    write_message(&mut input, &Message::Binary(vec![0; 126])).await?;
    app.expect("SESSION_TOO_LARGE").await?;
    println!("Message sessions passed");
    Ok(())
}
