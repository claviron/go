//! Explicit cross-process acceptance for HTTP and invoke in one Go process.
use super::{App, Result, exchange, frame};
use claviron::orbit::{
    invoke::{CallError, Client, Failure},
    link::{
        Connection,
        application::{ApplicationClient, ApplicationSegment, LINK_SPEC},
        invoke::Limits,
    },
};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot},
};

async fn lines(
    app: &mut App,
    expected: &[&str],
) -> Result<()> {
    let mut expected: BTreeSet<_> =
        expected.iter().map(|s| (*s).to_owned()).collect();
    while !expected.is_empty() {
        let line =
            app.output.next_line().await?.ok_or("invoke output ended")?;
        if !expected.remove(&line) {
            return Err(format!(
                "unexpected invoke output {line:?}; pending {expected:?}"
            )
            .into());
        }
    }
    Ok(())
}
async fn start(
    binary: &PathBuf,
    http: &str,
    invoke: &str,
) -> Result<App> {
    let mut child = Command::new(binary)
        .arg(http)
        .arg(invoke)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let input = child.stdin.take().ok_or("invoke stdin")?;
    let mut output =
        BufReader::new(child.stdout.take().ok_or("invoke stdout")?).lines();
    let ready = output
        .next_line()
        .await?
        .ok_or("invoke process exited before ready")?;
    let lane = ready
        .strip_prefix("READY ")
        .ok_or("invoke readiness missing")?
        .parse()?;
    Ok(App { child, input, output, lane })
}
async fn call(
    client: &Client,
    op: &str,
    bytes: &[u8],
) -> claviron::orbit::invoke::CallResult<Vec<u8>> {
    client.call("go", op, bytes.to_vec()).await
}
pub async fn run(
    binary: &PathBuf,
    example: &PathBuf,
    fleet: &str,
    invoke_fleet: &str,
) -> Result<()> {
    if ApplicationSegment::existing_for_fleet(invoke_fleet)?.is_some() {
        return Err("invoke fleet already exists".into());
    }
    println!("OWNED_INVOKE_FLEET {invoke_fleet}");
    let limits = Limits {
        pending_calls: 16,
        accepted_calls: 16,
        operation_bytes: 64,
        request_bytes: 1024,
        response_bytes: 1024,
    };
    let connection =
        Connection::open(invoke_fleet, LINK_SPEC, "rust-peer", "test", 1)?;
    let (handle, driver) = connection.invocations(limits)?;
    let mut client = Client::new();
    client.register("go", handle.route("go-calls")?)?;
    let client = Arc::new(client);
    let (holds, mut held) = mpsc::channel(16);
    let (stop, stopped) = oneshot::channel();
    let (ready, started) = oneshot::channel();
    let driving = tokio::spawn(async move {
        let driving = driver.run_until(
            async {
                let _ = stopped.await;
            },
            move |call| {
                let holds = holds.clone();
                async move {
                    match call.request.operation.as_str() {
                        "echo" => Ok(call.request.payload),
                        "hold" => {
                            let (send, receive) = oneshot::channel();
                            holds
                                .send(send)
                                .await
                                .map_err(|_| Failure::Closing)?;
                            let _ = receive.await;
                            Ok(b"released".to_vec())
                        }
                        _ => Err(Failure::Unsupported),
                    }
                }
            },
            |_| async { Ok(()) },
        );
        tokio::pin!(driving);
        let mut ready = Some(ready);
        std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(driving.as_mut(), cx);
            if let Some(ready) = ready.take() {
                let _ = ready.send(result.is_pending());
            }
            result
        })
        .await
    });
    assert!(started.await?);
    let mut app = start(binary, fleet, invoke_fleet).await?;
    let pid = app.child.id().ok_or("invoke pid")?;
    let http = ApplicationClient::join(fleet, "invoke-http", 1)?;
    let segment = ApplicationSegment::for_fleet(fleet)?;
    assert_eq!(call(&client, "echo", &[0, 255, 4]).await?, [0, 255, 4]);
    assert_eq!(
        call(&client, "reject", &[0, 255]).await,
        Err(CallError::Remote(Failure::Rejected(vec![0, 255])))
    );
    assert_eq!(
        call(&client, "unknown", &[]).await,
        Err(CallError::Remote(Failure::Unsupported))
    );
    assert_eq!(
        call(&client, "oversize", &[]).await,
        Err(CallError::Remote(Failure::ResponseTooLarge))
    );
    assert_eq!(
        call(&client, "panic", &[]).await,
        Err(CallError::Remote(Failure::Rejected(vec![])))
    );
    assert_eq!(call(&client, "generation", &[]).await?, b"1");
    let (head, bytes) = exchange(&http, frame("/"), b"").await?;
    assert_eq!(bytes, b"1");
    assert!(
        head.headers
            .iter()
            .any(|(key, value)| key.eq_ignore_ascii_case(b"x-pid")
                && value == pid.to_string().as_bytes())
    );
    println!(
        "PASS HTTP and typed invoke outcomes share one Go process and separate Links"
    );

    app.command("capacity-call").await?;
    let capacity_one = held.recv().await.ok_or("first capacity call")?;
    app.command("capacity-call").await?;
    let capacity_two = held.recv().await.ok_or("second capacity call")?;
    app.command("capacity-probe").await?;
    app.expect("CALL_BUSY").await?;
    assert_eq!(exchange(&http, frame("/"), b"").await?.1, b"1");
    capacity_one.send(()).map_err(|_| "first capacity gate")?;
    capacity_two.send(()).map_err(|_| "second capacity gate")?;
    app.expect("CAPACITY_DONE").await?;
    app.expect("CAPACITY_DONE").await?;
    println!(
        "PASS invoke pending capacity is bounded while HTTP continues independently"
    );

    app.command("call-cancel").await?;
    let release = held.recv().await.ok_or("Rust handler not accepted")?;
    app.command("cancel-call").await?;
    app.expect("CALL_CANCELLED").await?;
    // The remote operation still owns its gate after caller cancellation.
    release.send(()).map_err(|_| "remote work was rolled back")?;
    assert_eq!(exchange(&http, frame("/"), b"").await?.1, b"1");
    println!(
        "PASS Go caller cancellation withdraws interest without cancelling remote work"
    );

    let caller = client.clone();
    let retired = tokio::spawn(async move { call(&caller, "hold", &[]).await });
    app.expect("INCOMING 1").await?;
    app.command("call-generation").await?;
    let release = held.recv().await.ok_or("generation call not accepted")?;
    app.command("restart").await?;
    lines(&mut app, &["RESTARTED", "IN_CANCELLED 1", "CALL_CANCELLED"]).await?;
    assert_eq!(retired.await?, Err(CallError::Remote(Failure::Closing)));
    release
        .send(())
        .map_err(|_| "generation cancellation rolled back remote work")?;
    assert_eq!(call(&client, "generation", &[]).await?, b"2");
    assert_eq!(exchange(&http, frame("/"), b"").await?.1, b"2");
    app.command("release-late").await?;
    app.expect("LATE_RETURN 1").await?;
    assert_eq!(call(&client, "generation", &[]).await?, b"2");
    app.command("stop").await?;
    app.expect("STOPPED").await?;
    assert_eq!(
        call(&client, "generation", &[]).await,
        Err(CallError::Remote(Failure::Closing))
    );
    assert_eq!(exchange(&http, frame("/"), b"").await?.0.status, 503);
    app.command("start").await?;
    app.expect("STARTED").await?;
    assert_eq!(call(&client, "generation", &[]).await?, b"3");
    assert_eq!(app.child.id(), Some(pid));
    println!(
        "PASS stop/start/restart retire inbound and outbound generations without restarting Go; late replies stay retired"
    );

    // Both protocols have accepted work before application shutdown.
    let pending = http.send("app", frame("/hold-invoke"))?;
    let (_, mut body, reply, _) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    app.expect("HTTP_HELD").await?;
    let caller = client.clone();
    let draining =
        tokio::spawn(
            async move { call(&caller, "drain", b"from-invoke").await },
        );
    app.expect("DRAIN_HELD").await?;
    let mut watcher = segment.link().watch("iv-drain")?;
    app.command("shutdown").await?;
    while segment.inbox().consumers(app.lane)? > 0 {
        watcher.next().await?;
    }
    app.command("release-http").await?;
    let mut bytes = Vec::new();
    body.read_to_end(&mut bytes).await?;
    assert_eq!(bytes, b"from-http");
    app.command("withdraw-invoke").await?;
    app.expect("INVOKE_WITHDRAWN").await?;
    assert_eq!(
        call(&client, "echo", b"new").await,
        Err(CallError::Remote(Failure::Closing))
    );
    app.command("release-drain").await?;
    assert_eq!(draining.await??, b"from-invoke");
    app.expect("CLOSED").await?;
    app.command("finish").await?;
    assert!(app.child.wait().await?.success());
    println!(
        "PASS graceful drain retains accepted HTTP/invoke nested calls, rejects new intake and respects wait-only context cancellation"
    );

    // Explicit application Close cancels invoke without terminating Go itself.
    let mut closing = start(binary, fleet, invoke_fleet).await?;
    let caller = client.clone();
    let cancelled =
        tokio::spawn(async move { call(&caller, "cancel-close", &[]).await });
    closing.expect("CLOSE_HELD").await?;
    closing.command("close").await?;
    closing.expect("CLOSE_CANCELLED").await?;
    assert_eq!(cancelled.await?, Err(CallError::Remote(Failure::Closing)));
    closing.expect("CLOSED").await?;
    assert!(closing.child.wait().await?.success());
    println!(
        "PASS explicit application Close cancels and joins invoke handlers"
    );

    // Exercise the shipped Go example too, not only the controllable fixture.
    let mut child = Command::new(example)
        .args([
            "--fleet",
            fleet,
            "--invoke-fleet",
            invoke_fleet,
            "--service",
            "go-calls",
            "--peer",
            "rust-peer",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut output =
        BufReader::new(child.stdout.take().ok_or("example stdout")?).lines();
    let ready = output.next_line().await?.ok_or("example readiness")?;
    assert!(ready.starts_with("READY "));
    assert_eq!(call(&client, "echo", b"from-rust").await?, b"from-rust");
    assert_eq!(
        exchange(&http, frame("/invoke"), b"").await?.1,
        b"hello from HTTP"
    );
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().ok_or("example pid")?.to_string())
        .status()
        .await?;
    assert!(status.success());
    assert!(child.wait().await?.success());
    println!(
        "PASS shipped Go example serves HTTP and invoke bidirectionally and drains after SIGTERM"
    );
    http.leave();
    drop(http);
    drop(watcher);
    drop(segment);
    drop(client);
    stop.send(()).map_err(|_| "peer driver ended early")?;
    driving.await??;
    Ok(())
}
