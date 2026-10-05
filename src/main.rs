mod chrome;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use bytes::Bytes;
use chromiumoxide::cdp::browser_protocol::input::{
    DispatchKeyEventParams, DispatchKeyEventType, DispatchMouseEventParams, DispatchMouseEventType,
    InsertTextParams,
};
use chromiumoxide::cdp::browser_protocol::page::{NavigateParams, ReloadParams};
use chrono::Timelike;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use chrome::Chrome;

const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const IDLE_LIMIT: Duration = Duration::from_secs(2 * 3600);

pub fn data_dir() -> PathBuf {
    PathBuf::from("/data")
}

#[derive(Clone, Serialize, PartialEq)]
pub struct Tab {
    id: String,
    title: String,
    url: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Bookmark {
    title: String,
    url: String,
}

#[derive(Default)]
struct Ui {
    tabs: Vec<Tab>,
    active: Option<String>,
    bookmarks: Vec<Bookmark>,
}

pub struct App {
    password: String,
    session: Mutex<Option<(String, Instant)>>,
    login_lock: tokio::sync::Mutex<()>,
    chrome: tokio::sync::Mutex<Option<Chrome>>,
    frames: watch::Sender<Bytes>,
    ui_json: watch::Sender<String>,
    selection: watch::Sender<String>,
    ui: Mutex<Ui>,
    clients: AtomicUsize,
    last_seen: Mutex<Instant>,
    viewport: Mutex<(i64, i64)>,
    conn_gen: watch::Sender<u64>,
}

impl App {
    pub fn has_clients(&self) -> bool {
        self.clients.load(Ordering::SeqCst) > 0
    }

    pub fn set_active(&self, id: String) {
        self.ui.lock().unwrap().active = Some(id);
        self.publish_ui();
    }

    pub fn set_tabs(&self, tabs: Vec<Tab>) {
        let mut ui = self.ui.lock().unwrap();
        if ui.tabs != tabs {
            ui.tabs = tabs;
            drop(ui);
            self.publish_ui();
        }
    }

    fn publish_ui(&self) {
        let ui = self.ui.lock().unwrap();
        let json = serde_json::json!({
            "t": "state",
            "tabs": ui.tabs,
            "active": ui.active,
            "bookmarks": ui.bookmarks,
        })
        .to_string();
        self.ui_json.send_if_modified(|cur| {
            let changed = *cur != json;
            *cur = json;
            changed
        });
    }

    fn save_bookmarks(&self) {
        let json = serde_json::to_vec_pretty(&self.ui.lock().unwrap().bookmarks).unwrap();
        let _ = std::fs::write(data_dir().join("bookmarks.json"), json);
        self.publish_ui();
    }

    fn authed(&self, headers: &HeaderMap) -> bool {
        let Some(token) = cookie(headers, "sid") else {
            return false;
        };
        matches!(&*self.session.lock().unwrap(),
            Some((t, at)) if *t == token && at.elapsed() < SESSION_TTL)
    }

    /// Starts Chrome if needed and makes sure frames are flowing.
    async fn ensure_chrome(self: &Arc<Self>) -> Result<(), String> {
        let mut guard = self.chrome.lock().await;
        if guard.as_ref().is_some_and(|c| c.dead.load(Ordering::SeqCst)) {
            guard.take().unwrap().shutdown().await;
        }
        if guard.is_none() {
            *guard = Some(Chrome::launch(self).await?);
        }
        let chrome = guard.as_mut().unwrap();
        if self.has_clients() && !chrome.is_casting() {
            chrome.start_cast(self).await;
        }
        Ok(())
    }

    async fn stop_chrome(&self) {
        let chrome = self.chrome.lock().await.take();
        if let Some(chrome) = chrome {
            chrome.shutdown().await;
            println!("Chrome stopped");
        }
    }

    async fn active_page(&self) -> Option<chromiumoxide::Page> {
        self.chrome.lock().await.as_ref()?.active_page()
    }
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_string())
}

fn random_hex(bytes: usize) -> String {
    (0..bytes).map(|_| format!("{:02x}", rand::random::<u8>())).collect()
}

fn quiet_hours() -> bool {
    let now = chrono::Utc::now().with_timezone(&chrono_tz::America::Los_Angeles);
    let minutes = now.hour() * 60 + now.minute();
    minutes >= 18 * 60 || minutes < 9 * 60 + 30
}

#[tokio::main]
async fn main() {
    let password = std::env::var("PASSWORD").ok().filter(|p| !p.is_empty()).unwrap_or_else(|| {
        let p = random_hex(12);
        println!("PASSWORD not set. Generated password for this run: {p}");
        p
    });
    let _ = std::fs::create_dir_all(data_dir().join("chrome-profile"));

    // Virtual display for the headful Chrome.
    let _xvfb = tokio::process::Command::new("Xvfb")
        .args([":99", "-screen", "0", "1920x1080x24", "-nolisten", "tcp"])
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| eprintln!("could not start Xvfb: {e}"))
        .ok();

    let bookmarks = std::fs::read(data_dir().join("bookmarks.json"))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();

    let app = Arc::new(App {
        password,
        session: Mutex::new(None),
        login_lock: tokio::sync::Mutex::new(()),
        chrome: tokio::sync::Mutex::new(None),
        frames: watch::Sender::new(Bytes::new()),
        ui_json: watch::Sender::new(String::new()),
        selection: watch::Sender::new(String::new()),
        ui: Mutex::new(Ui { bookmarks, ..Default::default() }),
        clients: AtomicUsize::new(0),
        last_seen: Mutex::new(Instant::now()),
        viewport: Mutex::new((1280, 800)),
        conn_gen: watch::Sender::new(0),
    });
    app.publish_ui();

    // Stop Chrome when nobody is connected and it's been idle 2h, or it's quiet hours.
    let monitor = app.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let idle = !monitor.has_clients()
                && (monitor.last_seen.lock().unwrap().elapsed() >= IDLE_LIMIT || quiet_hours());
            let crashed = monitor
                .chrome
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.dead.load(Ordering::SeqCst));
            if idle || crashed {
                monitor.stop_chrome().await;
            }
            if crashed && monitor.has_clients() {
                let _ = monitor.ensure_chrome().await;
            }
        }
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/ws", get(ws))
        .with_state(app.clone());

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    println!("Listening on port {port}");

    tokio::select! {
        r = axum::serve(listener, router) => { r.unwrap(); }
        _ = shutdown_signal() => {}
    }
    app.stop_chrome().await;
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn page(html: String) -> Response {
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
            (header::X_FRAME_OPTIONS, "DENY"),
        ],
        Html(html),
    )
        .into_response()
}

#[derive(Deserialize)]
struct IndexQuery {
    e: Option<String>,
}

async fn index(State(app): State<Arc<App>>, headers: HeaderMap, q: Query<IndexQuery>) -> Response {
    if app.authed(&headers) {
        page(include_str!("app.html").to_string())
    } else {
        let error = if q.e.is_some() { "Wrong password" } else { "" };
        page(include_str!("login.html").replace("{{ERROR}}", error))
    }
}

#[derive(Deserialize)]
struct LoginForm {
    password: String,
}

async fn login(State(app): State<Arc<App>>, headers: HeaderMap, Form(form): Form<LoginForm>) -> Response {
    // One attempt at a time, and a wrong password costs a second.
    let _guard = app.login_lock.lock().await;
    let ok = form.password.len() == app.password.len()
        && form.password.bytes().zip(app.password.bytes()).fold(0, |acc, (a, b)| acc | (a ^ b)) == 0;
    if !ok {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return Redirect::to("/?e=1").into_response();
    }

    let token = random_hex(32);
    *app.session.lock().unwrap() = Some((token.clone(), Instant::now()));
    app.conn_gen.send_modify(|g| *g += 1); // one session at a time
    let https = headers.get("x-forwarded-proto").is_some_and(|v| v == "https");
    let cookie = format!(
        "sid={token}; Path=/; HttpOnly; SameSite=Strict{}",
        if https { "; Secure" } else { "" }
    );
    ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
}

async fn logout(State(app): State<Arc<App>>) -> Response {
    *app.session.lock().unwrap() = None;
    app.conn_gen.send_modify(|g| *g += 1);
    (
        [(header::SET_COOKIE, "sid=; Path=/; Max-Age=0")],
        Redirect::to("/"),
    )
        .into_response()
}

async fn ws(State(app): State<Arc<App>>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let origin_host = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .and_then(|o| o.split_once("://"))
        .map(|(_, h)| h);
    if !app.authed(&headers) || host.is_none() || host != origin_host {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    upgrade.on_upgrade(move |socket| client(app, socket))
}

async fn client(app: Arc<App>, socket: WebSocket) {
    let my_gen = {
        app.conn_gen.send_modify(|g| *g += 1);
        *app.conn_gen.borrow()
    };
    let mut gen_rx = app.conn_gen.subscribe();
    app.clients.fetch_add(1, Ordering::SeqCst);
    let (mut tx, mut rx) = socket.split();

    let _ = tx.send(Message::Text(r#"{"t":"status","msg":"Loading…"}"#.into())).await;
    if let Err(e) = app.ensure_chrome().await {
        eprintln!("{e}");
        let msg = serde_json::json!({"t": "status", "msg": e}).to_string();
        let _ = tx.send(Message::Text(msg.into())).await;
    } else {
        let mut frames = app.frames.subscribe();
        let mut ui = app.ui_json.subscribe();
        let mut selection = app.selection.subscribe();
        frames.mark_changed();
        ui.mark_changed();

        let sender = async {
            loop {
                tokio::select! {
                    r = frames.changed() => {
                        if r.is_err() { break; }
                        let frame = frames.borrow_and_update().clone();
                        if !frame.is_empty() && tx.send(Message::Binary(frame)).await.is_err() { break; }
                    }
                    r = ui.changed() => {
                        if r.is_err() { break; }
                        let json = ui.borrow_and_update().clone();
                        if tx.send(Message::Text(json.into())).await.is_err() { break; }
                    }
                    r = selection.changed() => {
                        if r.is_err() { break; }
                        let text = selection.borrow_and_update().clone();
                        let json = serde_json::json!({"t": "sel", "text": text}).to_string();
                        if tx.send(Message::Text(json.into())).await.is_err() { break; }
                    }
                    _ = gen_rx.changed() => {
                        if *gen_rx.borrow_and_update() != my_gen {
                            let _ = tx.send(Message::Text(r#"{"t":"replaced"}"#.into())).await;
                            break;
                        }
                    }
                }
            }
        };
        let receiver = async {
            while let Some(Ok(msg)) = rx.next().await {
                if let Message::Text(text) = msg {
                    match serde_json::from_str::<Cmd>(&text) {
                        Ok(cmd) => handle(&app, cmd).await,
                        Err(e) => eprintln!("bad message: {e}"),
                    }
                }
            }
        };
        tokio::select! {
            _ = sender => {}
            _ = receiver => {}
        }
    }

    if app.clients.fetch_sub(1, Ordering::SeqCst) == 1 {
        *app.last_seen.lock().unwrap() = Instant::now();
        if let Some(chrome) = app.chrome.lock().await.as_mut() {
            chrome.stop_cast().await;
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum Cmd {
    Size { w: i64, h: i64 },
    Mouse { p: DispatchMouseEventParams },
    Key { p: DispatchKeyEventParams },
    Text { text: String },
    Go { url: String },
    Back,
    Forward,
    Reload,
    TabNew,
    TabSelect { id: String },
    TabClose { id: String },
    BmAdd,
    BmDel { url: String },
    BmRename { url: String, title: String },
}

/// Text currently selected in the page, including inside text fields and
/// same-origin iframes. Password fields are never read.
const SELECTION_JS: &str = r#"(() => {
  let doc = document, el = doc.activeElement;
  while (el && el.tagName === 'IFRAME' && el.contentDocument) { doc = el.contentDocument; el = doc.activeElement; }
  if (el && (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT')) {
    if (el.type === 'password' || el.selectionStart == null) return '';
    return el.value.substring(el.selectionStart, el.selectionEnd);
  }
  return String(doc.getSelection() || '');
})()"#;

/// Sends the page's selected text to the client so Ctrl+C can copy it locally.
fn publish_selection(app: &Arc<App>, page: chromiumoxide::Page) {
    let app = app.clone();
    tokio::spawn(async move {
        if let Ok(text) = page.evaluate(SELECTION_JS).await.and_then(|v| Ok(v.into_value::<String>()?)) {
            app.selection.send_if_modified(|cur| {
                let changed = *cur != text;
                *cur = text;
                changed
            });
        }
    });
}

fn to_url(input: &str) -> String {
    let s = input.trim();
    if s.contains("://") || s.starts_with("about:") || s.starts_with("data:") {
        s.to_string()
    } else if s.contains('.') && !s.contains(' ') {
        format!("https://{s}")
    } else {
        let q = encode_query(s);
        format!("https://duckduckgo.com/?q={q}")
    }
}

fn encode_query(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            b' ' => "+".to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

async fn handle(app: &Arc<App>, cmd: Cmd) {
    match cmd {
        Cmd::Size { w, h } => {
            let (w, h) = (w.clamp(200, 3840), h.clamp(200, 2160));
            *app.viewport.lock().unwrap() = (w, h);
            if let Some(chrome) = app.chrome.lock().await.as_ref() {
                chrome.resize(w, h).await;
            }
        }
        Cmd::Mouse { p } => {
            if let Some(page) = app.active_page().await {
                let released = p.r#type == DispatchMouseEventType::MouseReleased;
                let _ = page.execute(p).await;
                if released {
                    publish_selection(app, page);
                }
            }
        }
        Cmd::Key { p } => {
            if let Some(page) = app.active_page().await {
                let released = p.r#type == DispatchKeyEventType::KeyUp;
                let _ = page.execute(p).await;
                if released {
                    publish_selection(app, page);
                }
            }
        }
        Cmd::Text { text } => {
            if let Some(page) = app.active_page().await {
                let _ = page.execute(InsertTextParams::new(text)).await;
            }
        }
        Cmd::Go { url } => {
            if let Some(page) = app.active_page().await {
                // Navigation can take a while; don't block input.
                tokio::spawn(async move {
                    let _ = page.execute(NavigateParams::new(to_url(&url))).await;
                });
            }
        }
        Cmd::Back | Cmd::Forward => {
            let js = if matches!(cmd, Cmd::Back) { "history.back()" } else { "history.forward()" };
            if let Some(page) = app.active_page().await {
                tokio::spawn(async move {
                    let _ = page.evaluate(js).await;
                });
            }
        }
        Cmd::Reload => {
            if let Some(page) = app.active_page().await {
                tokio::spawn(async move {
                    let _ = page.execute(ReloadParams::default()).await;
                });
            }
        }
        Cmd::TabNew => {
            if let Some(chrome) = app.chrome.lock().await.as_mut() {
                chrome.new_tab(app).await;
            }
        }
        Cmd::TabSelect { id } => {
            if let Some(chrome) = app.chrome.lock().await.as_mut() {
                chrome.activate_id(app, &id).await;
            }
        }
        Cmd::TabClose { id } => {
            if let Some(chrome) = app.chrome.lock().await.as_mut() {
                chrome.close_tab(&id).await;
            }
        }
        Cmd::BmAdd => {
            let mut ui = app.ui.lock().unwrap();
            let current = ui.tabs.iter().find(|t| Some(&t.id) == ui.active.as_ref()).cloned();
            if let Some(tab) = current {
                if tab.url != "about:blank" && !ui.bookmarks.iter().any(|b| b.url == tab.url) {
                    let title = if tab.title.is_empty() { tab.url.clone() } else { tab.title };
                    ui.bookmarks.push(Bookmark { title, url: tab.url });
                    drop(ui);
                    app.save_bookmarks();
                }
            }
        }
        Cmd::BmDel { url } => {
            app.ui.lock().unwrap().bookmarks.retain(|b| b.url != url);
            app.save_bookmarks();
        }
        Cmd::BmRename { url, title } => {
            let title = title.trim();
            if !title.is_empty() {
                if let Some(b) = app.ui.lock().unwrap().bookmarks.iter_mut().find(|b| b.url == url) {
                    b.title = title.to_string();
                }
                app.save_bookmarks();
            }
        }
    }
}
