//! Isolated real-Link caller. The application under test is a Go executable.
mod invoke;
mod lifecycle;
use std::collections::BTreeSet;
use std::error::Error;
use std::future::poll_fn;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use claviron::orbit::link::application::{
    ApplicationBodies, ApplicationClient, ApplicationSegment, ControlCommand,
    Exchange, ExchangeReply, HttpVersion, LINK_BUFFER_BYTES, ShutdownNotice,
};
use tokio::io::{
    AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines,
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
        Self::start_mode(binary, fleet, name, false).await
    }
    async fn start_mode(
        binary: &PathBuf,
        fleet: &str,
        name: &str,
        stream: bool,
    ) -> Result<Self> {
        let mut command = Command::new(binary);
        command.arg(fleet).arg(name);
        if stream {
            command.arg("stream");
        }
        let mut child = command
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
    let (write, response) = tokio::join!(write, read);
    let response = response?;
    if response.0.status < 400 {
        write?;
    }

    Ok(response)
}

async fn run(
    binary: PathBuf,
    example: PathBuf,
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
        exchange(&client, frame("/consume"), &vec![0; 1025]).await?.0.status,
        413
    );
    let pending = client.send("app", frame("/abort"), false)?;
    let (_write, _response_body, head) = pending.split();
    assert!(head.head().await.is_err());
    println!(
        "PASS bounded bodies, HEAD, panic-to-500 and explicit handler abort"
    );

    let pending = client.send("app", frame("/panic-stream"), false)?;
    let (_, mut failed_body, failed_head) = pending.split();
    assert_eq!(failed_head.head().await?.status, 201);
    first.expect("PANIC_HEAD").await?;
    first.command("panic").await?;
    assert!(failed_body.read_to_end(&mut Vec::new()).await.is_err());
    println!(
        "PASS panic after a committed streaming head resets without rewriting status"
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
    lifecycle::fixtures(&binary, fleet, &client, &segment).await?;
    streaming(&binary, &example, fleet, &client, &segment).await?;
    client.leave();
    drop(client);
    drop(watcher);
    drop(output);
    drop(bodies);
    drop(segment);
    Ok(())
}

async fn streaming(
    binary: &PathBuf,
    example: &PathBuf,
    fleet: &str,
    client: &Arc<ApplicationClient>,
    segment: &ApplicationSegment,
) -> Result<()> {
    let mut app = App::start_mode(binary, fleet, "app", true).await?;
    // No input has been sent: reaching the Go handler proves intake is lazy.
    let pending = client.send("app", frame("/read-gate"), true)?;
    app.expect("READ_GATE").await?;
    let (mut input, mut output, reply) = pending.split();
    input.write_all(&vec![b'x'; LINK_BUFFER_BYTES]).await?;
    poll_fn(|cx| match Pin::new(&mut input).poll_write(cx, b"x") {
        Poll::Pending => Poll::Ready(Ok::<_, Box<dyn Error + Send + Sync>>(())),
        Poll::Ready(result) => Poll::Ready(Err(format!(
            "upload did not backpressure: {result:?}"
        )
        .into())),
    })
    .await?;
    assert_eq!(exchange(client, frame("/"), b"").await?.1, b"hello");
    app.command("release").await?;
    let upload = async {
        input.write_all(&vec![b'x'; LINK_BUFFER_BYTES * 2]).await?;
        input.shutdown().await
    };
    let download = async {
        assert_eq!(reply.head().await?.status, 200);
        let mut data = Vec::new();
        output.read_to_end(&mut data).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(data)
    };
    let (upload, download) = tokio::join!(upload, download);
    upload?;
    assert_eq!(download?, vec![b'x'; LINK_BUFFER_BYTES * 3]);
    println!(
        "PASS lazy Go body reads, a provably full upload window, full-duplex flow and unrelated-request progress"
    );

    let pending = client.send("app", frame("/read-close"), true)?;
    app.expect("READ_CLOSE_WAIT").await?;
    app.command("close-body").await?;
    app.expect("READ_CLOSED").await?;
    let (_, mut output, reply) = pending.split();
    assert_eq!(reply.head().await?.status, 204);
    output.read_to_end(&mut Vec::new()).await?;
    println!(
        "PASS Body.Close interrupts a pending Read without cancelling the response"
    );

    let pending = client.send("app", frame("/read-cancel"), true)?;
    app.expect("READ_WAIT").await?;
    drop(pending);
    app.expect("READ_CANCELLED").await?;
    let mut request = frame("/write-cancel");
    request.query = format!("window={LINK_BUFFER_BYTES}").into_bytes();
    let pending = client.send("app", request, false)?;
    let (_, output, reply) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    app.expect("WRITE_WINDOW").await?;
    assert_eq!(exchange(client, frame("/"), b"").await?.1, b"hello");
    drop(output);
    app.expect("WRITE_CANCELLED").await?;
    println!(
        "PASS peer cancellation unblocks a pending native read and a full-window response write"
    );

    let pending = client.send("app", frame("/read-cancel"), true)?;
    app.expect("READ_WAIT").await?;
    app.command("close").await?;
    app.expect("READ_CANCELLED").await?;
    app.expect("CLOSED").await?;
    assert!(app.child.wait().await?.success());
    let (_, _response, reply) = pending.split();
    assert!(reply.head().await.is_err());
    println!(
        "PASS explicit Close joins pending I/O and cancels owned request contexts"
    );

    let mut drain = App::start_mode(binary, fleet, "app", true).await?;
    let mut request = frame("/write-drain");
    request.query = format!("window={LINK_BUFFER_BYTES}").into_bytes();
    let pending = client.send("app", request, false)?;
    let (_, mut output, reply) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    drain.expect("WRITE_WINDOW").await?;
    let mut watcher = segment.link().watch("drain")?;
    drain.command("shutdown").await?;
    while segment.inbox().consumers(drain.lane)? > 0 {
        watcher.next().await?;
    }
    let mut data = Vec::new();
    output.read_to_end(&mut data).await?;
    let mut expected = vec![b'w'; LINK_BUFFER_BYTES];
    expected.extend_from_slice(b"tail");
    assert_eq!(data, expected);
    drain.expect("CLOSED").await?;
    assert!(drain.child.wait().await?.success());
    drop(watcher);
    println!(
        "PASS graceful drain retains a full-window response until its producer sends FIN"
    );
    example_streaming(example, fleet, client, segment).await?;
    Ok(())
}

async fn example_streaming(
    binary: &PathBuf,
    fleet: &str,
    client: &Arc<ApplicationClient>,
    segment: &ApplicationSegment,
) -> Result<()> {
    let mut child = Command::new(binary)
        .args(["--fleet", fleet, "--name", "app"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout =
        BufReader::new(child.stdout.take().ok_or("example stdout")?).lines();
    let ready = stdout.next_line().await?.ok_or("example did not join")?;
    let lane: usize =
        ready.rsplit_once(' ').ok_or("example readiness")?.1.parse()?;
    let mut request = frame("/echo");
    request.method = "POST".into();
    let pending = client.send("app", request, true)?;
    let (mut input, mut output, reply) = pending.split();
    input.write_all(b"first").await?;
    assert_eq!(reply.head().await?.status, 200);
    let mut prefix = [0u8; 5];
    output.read_exact(&mut prefix).await?;
    assert_eq!(&prefix, b"first");
    let upload = async {
        input.write_all(&vec![b'e'; LINK_BUFFER_BYTES * 3]).await?;
        input.shutdown().await
    };
    let download = async {
        let mut data = Vec::new();
        output.read_to_end(&mut data).await?;
        Ok::<_, std::io::Error>(data)
    };
    let (upload, download) = tokio::join!(upload, download);
    upload?;
    assert_eq!(download?, vec![b'e'; LINK_BUFFER_BYTES * 3]);
    println!(
        "PASS actual Go example: response bytes arrive before upload EOF, larger-than-window echo"
    );

    lifecycle::example(client, segment, lane, child.id().ok_or("example pid")?)
        .await?;

    let mut request = frame("/stream");
    request.query = format!("bytes={}", LINK_BUFFER_BYTES * 4).into_bytes();
    let pending = client.send("app", request, false)?;
    let (_, mut output, reply) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    let mut watcher = segment.link().watch("eg")?;
    let pid = child.id().ok_or("example pid")?.to_string();
    assert!(
        Command::new("kill").args(["-TERM", &pid]).status().await?.success()
    );
    while segment.inbox().consumers(lane)? > 0 {
        watcher.next().await?;
    }
    let mut body = Vec::new();
    output.read_to_end(&mut body).await?;
    assert_eq!(body, vec![b'g'; LINK_BUFFER_BYTES * 4]);
    assert!(child.wait().await?.success());
    println!(
        "PASS actual Go example: SIGTERM withdraws and drains a streaming response"
    );
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
    let example = std::env::args_os()
        .nth(3)
        .ok_or("Go example binary is required")?
        .into();
    run(binary, example, &fleet).await?;
    let invoke_binary = std::env::args_os()
        .nth(4)
        .ok_or("invoke fixture binary required")?
        .into();
    let invoke_fleet =
        std::env::args().nth(5).ok_or("invoke fleet required")?;
    let invoke_example =
        std::env::args_os().nth(6).ok_or("invoke example required")?.into();
    invoke::run(&invoke_binary, &invoke_example, &fleet, &invoke_fleet).await?;
    println!("Application acceptance passed");
    Ok(())
}
