//! `githubdw mcp`: the real binary serving the MCP protocol over stdio,
//! exercised end to end to cover the CLI's `--read-only` wiring (`cli.rs`'s
//! flag and `main.rs`'s `Command::Mcp` dispatch into `mcp::serve_stdio`).

mod support;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use support::Cli;

/// A running `githubdw mcp` child process, talking line-delimited JSON-RPC
/// over its stdin/stdout.
struct McpProcess {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
}

impl McpProcess {
    /// Spawn `githubdw --db <cli's temp db> mcp [--read-only]` and complete
    /// the MCP `initialize` handshake.
    fn spawn(cli: &Cli, read_only: bool) -> Self {
        let mut args = vec!["--db".to_string(), cli.db().to_string_lossy().into_owned()];
        args.push("mcp".to_string());
        if read_only {
            args.push("--read-only".to_string());
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_githubdw"))
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn githubdw mcp");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut process = Self {
            child,
            stdin: Some(stdin),
            stdout,
        };
        process.send(json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "cli_mcp test", "version": "0"}
            }
        }));
        let reply = process.receive();
        assert_eq!(reply["id"], 1, "{reply}");
        process.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        process
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("stdin still open");
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    /// Read the next JSON-RPC reply line, failing after five seconds so a
    /// hung server fails the test instead of the run.
    fn receive(&mut self) -> Value {
        let (tx, rx) = std::sync::mpsc::channel();
        // `BufReader::read_line` has no timeout, so poll it from a thread
        // scoped to this call and give up on the result if it is too slow.
        let mut line = String::new();
        let stdout_fd = &mut self.stdout;
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let read = stdout_fd.read_line(&mut line);
                let _ = tx.send(read);
            });
            rx.recv_timeout(Duration::from_secs(5))
                .expect("an mcp reply within 5s")
                .expect("a readable stdout");
        });
        serde_json::from_str(line.trim()).unwrap_or_else(|e| panic!("{e}: {line}"))
    }

    fn tool_names(&mut self, id: i64) -> Vec<String> {
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/list"}));
        let reply = self.receive();
        reply["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("a tools array: {reply}"))
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_string())
            .collect()
    }
}

impl McpProcess {
    /// Close stdin so the server's `serve_stdio` sees the client disconnect
    /// and exits on its own (the `stdio()` transport ends `waiting()` on
    /// EOF), then poll for the exit. A normal exit — unlike `kill()` — lets
    /// coverage instrumentation flush the binary's profile data.
    fn shut_down(&mut self) {
        self.stdin.take(); // drop closes the pipe, signaling EOF
        for _ in 0..50 {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for McpProcess {
    fn drop(&mut self) {
        self.shut_down();
    }
}

/// The default server (no flag) lists all five tools, `trigger_sync`
/// included.
#[test]
fn default_mode_lists_trigger_sync() {
    let cli = Cli::new();
    let mut mcp = McpProcess::spawn(&cli, false);
    let names = mcp.tool_names(2);
    assert!(names.contains(&"trigger_sync".to_string()), "{names:?}");
    assert_eq!(names.len(), 5, "{names:?}");
}

/// `--read-only` omits `trigger_sync` from `tools/list` and refuses a
/// direct call to it, driven through the real CLI flag end to end.
#[test]
fn read_only_flag_omits_and_refuses_trigger_sync() {
    let cli = Cli::new();
    let mut mcp = McpProcess::spawn(&cli, true);

    let names = mcp.tool_names(2);
    assert!(!names.contains(&"trigger_sync".to_string()), "{names:?}");
    assert_eq!(names.len(), 4, "{names:?}");

    mcp.send(json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "trigger_sync",
            "arguments": {"entity_type": "repo", "name": "octocat/hello", "days": 30}
        }
    }));
    let reply = mcp.receive();
    let error = reply
        .get("error")
        .unwrap_or_else(|| panic!("an error reply: {reply}"));
    assert!(
        error["message"].as_str().unwrap().contains("read-only"),
        "{error}"
    );
}
