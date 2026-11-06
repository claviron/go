//! Isolated real-Link caller. The application under test is a Go executable.
use std::collections::BTreeSet;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use claviron::orbit::link::application::{
    ApplicationBodies, ApplicationClient, ApplicationSegment, ControlCommand,
    Exchange, ExchangeReply, HttpVersion, ShutdownNotice,
};
use tokio::io::{
    AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines,
};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct App {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    lane: usize,
}

impl App {
    async fn start(
        binary: &PathBuf,
        fleet: &str,
        name: &str,
    ) -> Result<Self> {
        let mut child = Command::new(binary)
            .arg(fleet)
            .arg(name)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("child stdin")?;
        let mut output =
            BufReader::new(child.stdout.take().ok_or("child stdout")?).lines();
        let ready = output
            .next_line()
            .await?
            .ok_or("application exited before ready")?;
        let lane = ready
            .strip_prefix("READY ")
            .ok_or("application did not join")?
            .parse()?;
        Ok(Self { child, input, output, lane })
    }
    async fn expect(
        &mut self,
        wanted: &str,
    ) -> Result<()> {
        let line =
            self.output.next_line().await?.ok_or("application output ended")?;
        if line != wanted {
            return Err(format!("expected {wanted:?}, got {line:?}").into());
        }
        Ok(())
    }
    async fn command(
        &mut self,
        command: &str,
    ) -> Result<()> {
        self.input.write_all(command.as_bytes()).await?;
        self.input.write_all(b"\n").await?;
        Ok(())
    }
    async fn close(mut self) -> Result<()> {
        self.command("close").await?;
        self.expect("CLOSED").await?;
        if !self.child.wait().await?.success() {
            return Err("Go application failed".into());
        }
        Ok(())
    }
}

fn frame(path: &str) -> Exchange {
    Exchange {
        method: "GET".into(),
        scheme: "https".into(),
        version: HttpVersion::Http11,
        path: path.into(),
        raw_path: path.as_bytes().to_vec(),
        headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
        client: Some(([192, 0, 2, 10], 1234).into()),
        server: Some(([127, 0, 0, 1], 443).into()),
        ..Default::default()
    }
}

async fn exchange(
    client: &ApplicationClient,
    request: Exchange,
    body: &[u8],
) -> Result<(ExchangeReply, Vec<u8>)> {
    let pending = client.send("app", request, !body.is_empty())?;
    let (mut input, mut output, reply) = pending.split();
    let write = async {
        if !body.is_empty() {
            input.write_all(body).await?;
            input.shutdown().await?;
        }
        Ok::<_, std::io::Error>(())
    };
    let read = async {
        let head = reply.head().await?;
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>((head, bytes))
    };
    let (_, response) = tokio::try_join!(
        async {
            write.await.map_err(|error| {
                Box::new(error) as Box<dyn Error + Send + Sync>
            })
        },
        read
    )?;
    Ok(response)
}

async fn run(
    binary: PathBuf,
    fleet: &str,
) -> Result<()> {
    let mut first = App::start(&binary, fleet, "app").await?;
    let segment = ApplicationSegment::for_fleet(fleet)?;
    assert_eq!(segment.inbox().role(first.lane)?.as_deref(), Some("go"));
    let client = ApplicationClient::join(fleet, "acceptance-caller", 1)?;
    assert!(client.serves("app"));

    let mut request = frame("/inspect%2Fname");
    request.method = "POST".into();
    request.path = "/inspect/name".into();
    request.query = b"q=a%20b&x=1".to_vec();
    request.headers.extend([
        (b"x-Test".to_vec(), b"first".to_vec()),
        (b"X-Test".to_vec(), b"second".to_vec()),
    ]);
    let (head, body) = exchange(&client, request, b"payload").await?;
    assert_eq!(head.status, 201);
    let peer = client
        .applications()
        .into_iter()
        .find(|peer| peer.lane == first.lane)
        .ok_or("application peer missing")?;
    assert!(client.application_state(&peer).is_some());
    assert_eq!(
        head.headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case(b"set-cookie"))
            .count(),
        2
    );
    assert_eq!(body, b"POST|/inspect%2Fname?q=a%20b&x=1|example.test|https|[first second]|payload");
    println!(
        "PASS Go handler: method, raw URI/query, ordered/repeated headers, body and addresses"
    );

    assert_eq!(exchange(&client, frame("/panic"), b"").await?.0.status, 500);
    assert_eq!(exchange(&client, frame("/large"), b"").await?.0.status, 500);
    let mut head_request = frame("/");
    head_request.method = "HEAD".into();
    let (head, body) = exchange(&client, head_request, b"").await?;
    assert!(body.is_empty());
    assert!(
        head.headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(b"content-length")
                && value == b"5")
    );
    assert_eq!(
        exchange(&client, frame("/"), &vec![0; 1025]).await?.0.status,
        413
    );
    let pending = client.send("app", frame("/abort"), false)?;
    let (_write, _response_body, head) = pending.split();
    assert!(head.head().await.is_err());
    println!(
        "PASS bounded bodies, HEAD, panic-to-500 and explicit handler abort"
    );

    let pending = client.send("app", frame("/wait"), false)?;
    first.expect("WAIT").await?;
    drop(pending);
    first.expect("CANCELLED").await?;
    println!(
        "PASS peer cancellation reaches Request.Context without application I/O"
    );

    // Failed head publication must reset the body and retire the exchange,
    // while the application remains able to answer later requests.
    let bodies = ApplicationBodies::for_segment(&segment, client.lane(), 1)?;
    let (mut endpoint, ticket) = bodies.create()?;
    endpoint.finish()?;
    let (mut output, _) = endpoint.split();
    let mut request = frame("/");
    request.origin = u16::MAX;
    request.body = Some(ticket);
    let target = segment.inbox().targets("app")[0].0;
    segment.inbox().write(target, &request.encode()?)?;
    assert!(output.read_to_end(&mut Vec::new()).await.is_err());
    assert_eq!(exchange(&client, frame("/"), b"").await?.1, b"hello");
    println!(
        "PASS failed reply publication resets the stream without poisoning the application"
    );

    let second = App::start(&binary, fleet, "app").await?;
    let mut ids = BTreeSet::new();
    let mut work = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let client = Arc::clone(&client);
        work.spawn(async move { exchange(&client, frame("/"), b"").await });
    }
    while let Some(result) = work.join_next().await {
        let (head, body) = result??;
        assert_eq!(body, b"hello");
        ids.insert(
            head.headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(b"x-instance"))
                .ok_or("instance header missing")?
                .1
                .clone(),
        );
    }
    assert_eq!(ids.len(), 2);
    let second_lane = second.lane;
    second.close().await?;
    assert_eq!(segment.inbox().consumers(second_lane)?, 0);
    assert!(segment.inbox().consumers(first.lane)? > 0);
    assert_eq!(exchange(&client, frame("/"), b"").await?.1, b"hello");
    println!(
        "PASS concurrent requests, two same-name Go processes and isolated lane release"
    );

    let pending = client.send("app", frame("/drain"), false)?;
    first.expect("DRAIN").await?;
    segment.inbox().write(
        first.lane,
        &ShutdownNotice { edge: "another-edge".into() }.encode()?,
    )?;
    assert_eq!(exchange(&client, frame("/diagnostics"), b"").await?.1, b"1");
    let mut watcher = segment.link().watch("test")?;
    first.command("shutdown").await?;
    while segment.inbox().consumers(first.lane)? > 0 {
        watcher.next().await?;
    }
    assert!(!client.serves("app"));
    first.command("release").await?;
    let (_, mut output, reply) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    let mut body = Vec::new();
    output.read_to_end(&mut body).await?;
    assert_eq!(body, b"drained");
    first.expect("CLOSED").await?;
    assert!(first.child.wait().await?.success());
    assert!(segment.inbox().consumers(client.lane())? > 0);
    println!(
        "PASS graceful withdrawal drains admitted work and preserves the caller lane"
    );

    let control = App::start(&binary, fleet, "app").await?;
    segment.control(
        "app",
        &ControlCommand {
            application: "app".into(),
            name: "restart".into(),
            payload: Vec::new(),
            target: None,
        },
    )?;
    assert_eq!(exchange(&client, frame("/diagnostics"), b"").await?.1, b"1");
    assert_eq!(exchange(&client, frame("/"), b"").await?.1, b"hello");
    control.close().await?;
    println!(
        "PASS unsupported management is observable and never executed; foreign shutdown does not cancel admitted work"
    );
    client.leave();
    drop(client);
    drop(watcher);
    drop(output);
    drop(bodies);
    drop(segment);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let binary = std::env::args_os()
        .nth(1)
        .ok_or("usage: orbit-go-acceptance <Go fixture binary>")?
        .into();
    let fleet =
        std::env::args().nth(2).ok_or("an isolated fleet name is required")?;
    if ApplicationSegment::existing_for_fleet(&fleet)?.is_some() {
        return Err("isolated fleet already exists".into());
    }
    println!("OWNED_FLEET {fleet}");
    run(binary, &fleet).await?;
    println!("Application acceptance passed");
    Ok(())
}
