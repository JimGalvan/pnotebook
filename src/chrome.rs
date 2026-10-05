use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use chromiumoxide::cdp::browser_protocol::page::{
    EventScreencastFrame, ScreencastFrameAckParams, StartScreencastFormat, StartScreencastParams,
    StopScreencastParams,
};
use chromiumoxide::cdp::browser_protocol::target::{GetTargetsParams, TargetId};
use chromiumoxide::{Browser, BrowserConfig, Page};
use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::{App, Tab, Viewport, data_dir};

const CHROME: &str = "/usr/bin/chromium";
const FRAME_INTERVAL: Duration = Duration::from_millis(33); // ~30 fps cap

pub struct Chrome {
    browser: Browser,
    pub dead: Arc<AtomicBool>,
    handler: JoinHandle<()>,
    poll: Option<JoinHandle<()>>,
    active: Option<Page>,
    pump: Option<JoinHandle<()>>,
    order: Vec<String>, // tab ids in the order they were opened
    saved_urls: Vec<String>,
}

impl Chrome {
    pub async fn launch(app: &Arc<App>) -> Result<Chrome, String> {
        let profile = data_dir().join("chrome-profile");
        // A crashed Chrome leaves these behind and then refuses to start.
        for f in ["SingletonLock", "SingletonSocket", "SingletonCookie"] {
            let _ = std::fs::remove_file(profile.join(f));
        }

        let config = BrowserConfig::builder()
            .chrome_executable(CHROME)
            .with_head() // headful on Xvfb: looks like a normal desktop Chrome to sites
            .no_sandbox()
            .disable_default_args()
            .respect_https_errors()
            .viewport(None)
            .window_size(1920, 1080)
            .user_data_dir(&profile)
            .env("DISPLAY", ":99")
            .args([
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--password-store=basic",
                "--use-mock-keychain",
                "--disable-blink-features=AutomationControlled",
                "--disk-cache-dir=/tmp/chrome-cache",
                "--hide-crash-restore-bubble",
                "--disable-features=TranslateUI",
            ])
            .build()?;

        let (browser, mut handler) = Browser::launch(config)
            .await
            .map_err(|e| format!("failed to launch Chrome: {e}"))?;

        let dead = Arc::new(AtomicBool::new(false));
        let flag = dead.clone();
        let handler = tokio::spawn(async move {
            while handler.next().await.is_some() {}
            flag.store(true, Ordering::SeqCst);
        });

        let mut chrome = Chrome {
            browser,
            dead,
            handler,
            poll: None,
            active: None,
            pump: None,
            order: Vec::new(),
            saved_urls: Vec::new(),
        };

        let _ = chrome.browser.fetch_targets().await;
        sleep(Duration::from_millis(300)).await;
        let initial = chrome.browser.pages().await.unwrap_or_default();

        // Reopen the tabs from last time, otherwise keep Chrome's initial tab.
        let saved: Vec<String> = std::fs::read(data_dir().join("tabs.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let mut first = None;
        for url in &saved {
            if let Ok(page) = chrome.browser.new_page(url.as_str()).await {
                first.get_or_insert(page);
            }
        }
        if first.is_some() {
            for page in initial {
                let _ = page.close().await;
            }
        } else {
            first = initial.into_iter().next();
        }
        let first = match first {
            Some(p) => p,
            None => chrome
                .browser
                .new_page("about:blank")
                .await
                .map_err(|e| format!("failed to open a tab: {e}"))?,
        };
        chrome.activate(app, first).await;

        let poll_app = app.clone();
        chrome.poll = Some(tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(500)).await;
                let mut guard = poll_app.chrome.lock().await;
                match guard.as_mut() {
                    Some(c) if !c.dead.load(Ordering::SeqCst) => c.sync_tabs(&poll_app).await,
                    _ => return,
                }
            }
        }));

        Ok(chrome)
    }

    pub fn is_casting(&self) -> bool {
        self.pump.is_some()
    }

    pub fn active_page(&self) -> Option<Page> {
        self.active.clone()
    }

    pub async fn activate(&mut self, app: &Arc<App>, page: Page) {
        if self.active.as_ref().map(|p| p.target_id()) == Some(page.target_id()) {
            return;
        }
        self.stop_cast().await;
        let _ = page.bring_to_front().await;
        let metrics = app.viewport.lock().unwrap().metrics();
        let _ = page.execute(metrics).await;
        let id = page.target_id().as_ref().to_string();
        self.active = Some(page);
        app.set_active(id);
        if app.has_clients() {
            self.start_cast(app).await;
        }
    }

    pub async fn activate_id(&mut self, app: &Arc<App>, id: &str) -> bool {
        match self.browser.get_page(TargetId::new(id)).await {
            Ok(page) => {
                self.activate(app, page).await;
                true
            }
            Err(_) => false,
        }
    }

    pub async fn new_tab(&mut self, app: &Arc<App>) {
        if let Ok(page) = self.browser.new_page("about:blank").await {
            self.order.push(page.target_id().as_ref().to_string());
            self.activate(app, page).await;
        }
    }

    pub async fn close_tab(&mut self, id: &str) {
        if let Ok(page) = self.browser.get_page(TargetId::new(id)).await {
            let _ = page.close().await;
        }
    }

    pub async fn resize(&self, viewport: Viewport) {
        if let Some(page) = &self.active {
            let _ = page.execute(viewport.metrics()).await;
        }
    }

    /// Streams JPEG frames of the active tab into `app.frames`.
    /// Frames are acked right away so Chrome never waits on the network;
    /// slow clients just skip to the newest frame.
    pub async fn start_cast(&mut self, app: &Arc<App>) {
        self.stop_cast().await;
        let Some(page) = self.active.clone() else {
            return;
        };
        let Ok(mut events) = page.event_listener::<EventScreencastFrame>().await else {
            return;
        };
        let frames = app.frames.clone();
        let ack_page = page.clone();
        self.pump = Some(tokio::spawn(async move {
            let mut next = Instant::now();
            while let Some(ev) = events.next().await {
                let data: &str = ev.data.as_ref();
                if let Ok(jpeg) = base64::engine::general_purpose::STANDARD.decode(data) {
                    frames.send_replace(Bytes::from(jpeg));
                }
                sleep_until(next).await;
                next = Instant::now() + FRAME_INTERVAL;
                let _ = ack_page
                    .execute(ScreencastFrameAckParams::new(ev.session_id))
                    .await;
            }
        }));
        let _ = page
            .execute(
                StartScreencastParams::builder()
                    .format(StartScreencastFormat::Jpeg)
                    .quality(70)
                    .every_nth_frame(1)
                    .build(),
            )
            .await;
    }

    pub async fn stop_cast(&mut self) {
        if let Some(pump) = self.pump.take() {
            pump.abort();
            if let Some(page) = &self.active {
                let _ = page.execute(StopScreencastParams::default()).await;
            }
        }
    }

    /// Polls Chrome's tab list: follows popups, recovers from closed tabs,
    /// updates the UI and saves open tabs to disk.
    async fn sync_tabs(&mut self, app: &Arc<App>) {
        let Ok(resp) = self.browser.execute(GetTargetsParams::default()).await else {
            return;
        };
        let mut tabs: Vec<Tab> = resp
            .result
            .target_infos
            .into_iter()
            .rev() // Chrome lists the newest tab first
            .filter(|t| t.r#type == "page" && !t.url.starts_with("devtools://"))
            .map(|t| Tab {
                id: t.target_id.as_ref().to_string(),
                title: t.title,
                url: t.url,
            })
            .collect();

        let first_sync = self.order.is_empty();
        let fresh: Vec<String> = tabs
            .iter()
            .filter(|t| !self.order.contains(&t.id))
            .map(|t| t.id.clone())
            .collect();
        if first_sync {
            self.order.extend(fresh);
        } else if let Some(id) = fresh.last() {
            // A page opened a new tab (target=_blank, window.open): switch to it.
            if self.activate_id(app, id).await {
                self.order.extend(fresh);
            }
        }
        self.order.retain(|id| tabs.iter().any(|t| &t.id == id));
        tabs.sort_by_key(|t| self.order.iter().position(|id| *id == t.id).unwrap_or(usize::MAX));

        let active_id = self.active.as_ref().map(|p| p.target_id().as_ref().to_string());
        if !tabs.iter().any(|t| Some(&t.id) == active_id.as_ref()) {
            self.active = None;
            match tabs.last() {
                Some(t) => {
                    let id = t.id.clone();
                    self.activate_id(app, &id).await;
                }
                None => self.new_tab(app).await,
            }
        }

        let urls: Vec<String> = tabs.iter().map(|t| t.url.clone()).collect();
        if urls != self.saved_urls {
            if let Ok(json) = serde_json::to_vec(&urls) {
                let _ = std::fs::write(data_dir().join("tabs.json"), json);
            }
            self.saved_urls = urls;
        }
        app.set_tabs(tabs);
    }

    /// Closes Chrome gracefully so cookies and logins are flushed to disk.
    pub async fn shutdown(mut self) {
        if let Some(poll) = self.poll.take() {
            poll.abort();
        }
        self.stop_cast().await;
        let _ = timeout(Duration::from_secs(5), self.browser.close()).await;
        if timeout(Duration::from_secs(5), self.browser.wait()).await.is_err() {
            let _ = self.browser.kill().await;
        }
        self.handler.abort();
    }
}
