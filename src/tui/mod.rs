use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, BorderType, Paragraph, ListItem, List, Gauge};

use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;

use crate::sync::{EndpointConfig, SyncConfig, SyncProgress};

type ConnectResult = Result<(String, String, Vec<BucketInfo>), String>;

pub struct TuiConfig {
    pub do_api_key: Option<String>,
    pub shared_key: Option<String>,
    pub shared_secret: Option<String>,
    pub concurrency: usize,
    pub data_dir: String,
}

#[derive(PartialEq, Clone)]
enum Screen {
    Welcome,
    ApiKey,
    Connecting,
    SelectSource,
    SelectDest,
    Confirm,
    Syncing,
    Done,
}

#[derive(Clone)]
struct BucketInfo {
    name: String,
    region: String,
    is_cold: bool,
    object_count: u64,
    bytes_used: u64,
}

struct App {
    screen: Screen,
    step: u8,
    // Auth
    api_key: String,
    spaces_ak: String,
    spaces_sk: String,
    // Buckets
    buckets: Vec<BucketInfo>,
    source_idx: usize,
    dest_idx: usize,
    // Config
    concurrency: usize,
    data_dir: String,
    // State
    error: Option<String>,
    progress: Option<Arc<SyncProgress>>,
    sync_start: Option<Instant>,
    sync_elapsed: Option<Duration>,
    last_bytes: u64,
    last_check: Instant,
    current_speed: f64,
    // Background connect
    connect_result: Option<tokio::sync::oneshot::Receiver<ConnectResult>>,
    connect_status: Arc<ConnectStatus>,
}

#[derive(Default)]
struct ConnectStatus {
    cleaning_keys: std::sync::atomic::AtomicBool,
    keys_cleaned: std::sync::atomic::AtomicBool,
    creating_key: std::sync::atomic::AtomicBool,
    key_created: std::sync::atomic::AtomicBool,
    listing_buckets: std::sync::atomic::AtomicBool,
    buckets_listed: std::sync::atomic::AtomicBool,
    checking_types: std::sync::atomic::AtomicBool,
    buckets_checked: std::sync::atomic::AtomicI64,
    buckets_total: std::sync::atomic::AtomicI64,
    types_done: std::sync::atomic::AtomicBool,
}

const LOGO: &str = "\
 \u{2584} \u{2584}\u{2596}  \u{2584}\u{2596}  \u{259C}  \u{258C}  \u{2584}\u{2596}      \n\
 \u{258C}\u{258C}\u{258C}\u{258C}  \u{258C} \u{259B}\u{258C}\u{2590} \u{259B}\u{258C}  \u{259A} \u{258C}\u{258C}\u{259B}\u{258C}\u{259B}\u{2598}\n\
 \u{2599}\u{2598}\u{2599}\u{258C}  \u{2599}\u{2596}\u{2599}\u{258C}\u{2590}\u{2596}\u{2599}\u{258C}  \u{2584}\u{258C}\u{2599}\u{258C}\u{258C}\u{258C}\u{2599}\u{2596}\n\
                   \u{2584}\u{258C}      ";

pub async fn run(config: TuiConfig) -> Result<()> {
    let config_api_key = config.do_api_key.clone().unwrap_or_default();

    // Clean up any old do-cold-sync keys from previous runs
    if !config_api_key.is_empty() {
        cleanup_old_keys(&config_api_key).await;
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App {
        screen: Screen::Welcome,
        step: 0,
        api_key: config.do_api_key.unwrap_or_default(),
        spaces_ak: config.shared_key.unwrap_or_default(),
        spaces_sk: config.shared_secret.unwrap_or_default(),
        buckets: Vec::new(),
        source_idx: 0,
        dest_idx: 0,
        concurrency: config.concurrency,
        data_dir: config.data_dir,
        error: None,
        progress: None,
        sync_start: None,
        sync_elapsed: None,
        last_bytes: 0,
        last_check: Instant::now(),
        current_speed: 0.0,
        connect_result: None,
        connect_status: Arc::new(ConnectStatus::default()),
    };

    // Panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    loop {
        terminal.draw(|f| draw(f, &app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                    break;
                }
                if key.code == KeyCode::Char('q') && (app.screen == Screen::Done || app.screen == Screen::Welcome) {
                    break;
                }
                handle_input(&mut app, key.code);
            }
        }

        // Check for connect result
        if app.screen == Screen::Connecting {
            if let Some(ref mut rx) = app.connect_result {
                if let Ok(result) = rx.try_recv() {
                    app.connect_result = None;
                    match result {
                        Ok((ak, sk, buckets)) => {
                            app.spaces_ak = ak;
                            app.spaces_sk = sk;
                            app.buckets = buckets;
                            app.screen = Screen::SelectSource;
                            app.step = 2;
                        }
                        Err(e) => {
                            app.error = Some(e);
                            app.screen = Screen::ApiKey;
                            app.step = 1;
                        }
                    }
                }
            }
        }

        // Update speed calculation
        if app.screen == Screen::Syncing {
            if let Some(ref p) = app.progress {
                let now = Instant::now();
                let elapsed = now.duration_since(app.last_check).as_secs_f64();
                if elapsed >= 1.0 {
                    let current_bytes = p.bytes_synced.load(Ordering::Relaxed);
                    let delta = current_bytes.saturating_sub(app.last_bytes) as f64;
                    app.current_speed = delta / elapsed;
                    app.last_bytes = current_bytes;
                    app.last_check = now;
                }

                if p.done.load(Ordering::Relaxed) && app.screen != Screen::Done {
                    app.sync_elapsed = app.sync_start.map(|s| s.elapsed());
                    app.screen = Screen::Done;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

    // Clean up the Spaces key we created
    if !app.api_key.is_empty() && !app.spaces_ak.is_empty() {
        eprintln!("Cleaning up Spaces access key...");
        cleanup_key(&app.api_key, &app.spaces_ak).await;
    }

    Ok(())
}

fn handle_input(app: &mut App, key: KeyCode) {
    match app.screen {
        Screen::Welcome => {
            if key == KeyCode::Enter {
                app.screen = Screen::ApiKey;
                app.step = 1;
            }
        }
        Screen::ApiKey => handle_api_key_input(app, key),
        Screen::Connecting | Screen::Syncing => {}
        Screen::SelectSource | Screen::SelectDest => handle_bucket_select_input(app, key),
        Screen::Confirm => handle_confirm_input(app, key),
        Screen::Done => match key {
            KeyCode::Char('e') => {
                let path = format!("{}/sync-inventory.csv", app.data_dir);
                app.error = Some(format!("Inventory saved to {path}"));
            }
            KeyCode::Char('b') => {
                app.progress = None;
                app.sync_start = None;
                app.sync_elapsed = None;
                app.error = None;
                app.screen = Screen::SelectSource;
                app.step = 2;
            }
            _ => {}
        }
    }
}

fn handle_api_key_input(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Char(c) => app.api_key.push(c),
        KeyCode::Backspace => { app.api_key.pop(); }
        KeyCode::Esc => { app.screen = Screen::Welcome; app.step = 0; }
        KeyCode::Enter if !app.api_key.is_empty() => {
            app.error = None;
            app.screen = Screen::Connecting;
            app.connect_status = Arc::new(ConnectStatus::default());
            let api_key = app.api_key.clone();
            let status = app.connect_status.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            app.connect_result = Some(rx);
            tokio::spawn(async move {
                let result = do_connect_async(&api_key, &status).await;
                let _ = tx.send(result);
            });
        }
        _ => {}
    }
}

fn handle_bucket_select_input(app: &mut App, key: KeyCode) {
    if key == KeyCode::Char('r') {
        app.screen = Screen::Connecting;
        app.connect_status = Arc::new(ConnectStatus::default());
        let ak = app.spaces_ak.clone();
        let sk = app.spaces_sk.clone();
        let status = app.connect_status.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        app.connect_result = Some(rx);
        tokio::spawn(async move {
            status.listing_buckets.store(true, Ordering::Relaxed);
            match list_buckets_signed(&ak, &sk, &status).await {
                Ok(buckets) => { let _ = tx.send(Ok((ak, sk, buckets))); }
                Err(e) => { let _ = tx.send(Err(format!("Refresh failed: {e}"))); }
            }
        });
        return;
    }

    if app.screen == Screen::SelectSource {
        let std_buckets: Vec<usize> = app.buckets.iter().enumerate()
            .filter(|(_, b)| !b.is_cold).map(|(i, _)| i).collect();
        let pos = std_buckets.iter().position(|&i| i == app.source_idx).unwrap_or(0);
        match key {
            KeyCode::Up if pos > 0 => app.source_idx = std_buckets[pos - 1],
            KeyCode::Down if pos < std_buckets.len().saturating_sub(1) => app.source_idx = std_buckets[pos + 1],
            KeyCode::Enter if !std_buckets.is_empty() => {
                let cpus = std::thread::available_parallelism()
                    .map(std::num::NonZero::get).unwrap_or(2);
                app.concurrency = match cpus {
                    1 => 16,
                    2 => 32,
                    3..=4 => 48,
                    _ => 64,
                };
                app.dest_idx = app.buckets.iter()
                    .position(|b| b.is_cold).unwrap_or(0);
                app.screen = Screen::SelectDest;
                app.step = 3;
            }
            KeyCode::Esc => { app.screen = Screen::ApiKey; app.step = 1; }
            _ => {}
        }
    } else {
        let cold_buckets: Vec<usize> = app.buckets.iter().enumerate()
            .filter(|(_, b)| b.is_cold).map(|(i, _)| i).collect();
        let pos = cold_buckets.iter().position(|&i| i == app.dest_idx).unwrap_or(0);
        match key {
            KeyCode::Up if pos > 0 => app.dest_idx = cold_buckets[pos - 1],
            KeyCode::Down if pos < cold_buckets.len().saturating_sub(1) => app.dest_idx = cold_buckets[pos + 1],
            KeyCode::Enter if !cold_buckets.is_empty() => {
                app.screen = Screen::Confirm;
                app.step = 4;
            }
            KeyCode::Esc => { app.screen = Screen::SelectSource; app.step = 2; }
            _ => {}
        }
    }
}

fn handle_confirm_input(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Enter | KeyCode::Char('y') => {
            start_sync(app);
        }
        KeyCode::Up | KeyCode::Char('+' | '=') | KeyCode::Right => {
            let steps = [1,2,4,8,12,16,20,24,28,32,40,48,56,64,80,96,128,192,256];
            app.concurrency = steps.iter()
                .find(|&&s| s > app.concurrency)
                .copied()
                .unwrap_or(256);
        }
        KeyCode::Down | KeyCode::Char('-') | KeyCode::Left => {
            let steps = [1,2,4,8,12,16,20,24,28,32,40,48,56,64,80,96,128,192,256];
            app.concurrency = steps.iter().rev()
                .find(|&&s| s < app.concurrency)
                .copied()
                .unwrap_or(1);
        }
        KeyCode::Esc | KeyCode::Char('n') => { app.screen = Screen::SelectDest; app.step = 3; }
        _ => {}
    }
}

async fn do_connect_async(api_key: &str, status: &ConnectStatus) -> Result<(String, String, Vec<BucketInfo>), String> {
    // Clean up any leftover keys from previous runs first
    status.cleaning_keys.store(true, Ordering::Relaxed);
    cleanup_old_keys(api_key).await;
    status.keys_cleaned.store(true, Ordering::Relaxed);

    // Create fullaccess Spaces key via DO API
    status.creating_key.store(true, Ordering::Relaxed);
    let client = reqwest::Client::new();
    let resp = client.post("https://api.digitalocean.com/v2/spaces/keys")
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&serde_json::json!({
            "name": "do-cold-sync",
            "grants": [{"permission": "fullaccess"}]
        }))
        .send()
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("API error: {}", &body[..body.len().min(100)]));
    }

    let data: serde_json::Value = resp.json().await
        .map_err(|e| format!("Invalid response: {e}"))?;

    let ak = data["key"]["access_key"].as_str().unwrap_or("").to_string();
    let sk = data["key"]["secret_key"].as_str().unwrap_or("").to_string();

    if ak.is_empty() {
        return Err("Failed to create Spaces key".to_string());
    }
    status.key_created.store(true, Ordering::Relaxed);

    // List and classify buckets
    status.listing_buckets.store(true, Ordering::Relaxed);
    let buckets = list_buckets_signed(&ak, &sk, status).await
        .map_err(|e| format!("Failed to list buckets: {e}"))?;

    Ok((ak, sk, buckets))
}

async fn list_buckets_signed(ak: &str, sk: &str, status: &ConnectStatus) -> Result<Vec<BucketInfo>> {
    let s3 = crate::s3::S3Client::new(
        "https://nyc3.digitaloceanspaces.com".to_string(),
        "nyc3".to_string(),
        ak.to_string(),
        sk.to_string(),
        "do-cold-sync".to_string(),
    )?;

    // We need to add a list_buckets method to our S3 client
    // For now, make a signed GET / request
    let url = "https://nyc3.digitaloceanspaces.com/";
    let resp = s3.signed_get(url).await?;
    let body = resp.text().await?;

    // Parse XML
    let mut buckets = Vec::new();
    let mut in_bucket = false;
    let mut name = String::new();
    let mut region = String::new();
    let mut tag = String::new();

    let mut reader = Reader::from_str(&body);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(XmlEvent::Start(ref e)) => {
                let t = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if t == "Bucket" { in_bucket = true; name.clear(); region.clear(); }
                tag = t;
            }
            Ok(XmlEvent::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_bucket {
                    match tag.as_str() {
                        "Name" => name = text,
                        "BucketRegion" => region = text,
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::End(ref e)) => {
                if String::from_utf8_lossy(e.name().as_ref()) == "Bucket" {
                    if !name.is_empty() {
                        buckets.push(BucketInfo {
                            name: name.clone(),
                            region: region.clone(),
                            is_cold: false,
                            object_count: 0,
                            bytes_used: 0,
                        });
                    }
                    in_bucket = false;
                }
            }
            Ok(XmlEvent::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    status.buckets_listed.store(true, Ordering::Relaxed);
    status.checking_types.store(true, Ordering::Relaxed);
    status.buckets_total.store(buckets.len() as i64, Ordering::Relaxed);

    // HEAD each bucket to get type, object count, and size
    for bucket in &mut buckets {
        let bucket_endpoint = format!("https://{}.{}.digitaloceanspaces.com", bucket.name, bucket.region);
        if let Ok(client) = crate::s3::S3Client::new(
            bucket_endpoint,
            bucket.region.clone(),
            ak.to_string(),
            sk.to_string(),
            "do-cold-sync".to_string(),
        ) {
            if let Ok((is_cold, objects, bytes)) = client.head_bucket_info().await {
                bucket.is_cold = is_cold;
                bucket.object_count = objects;
                bucket.bytes_used = bytes;
            }
            status.buckets_checked.fetch_add(1, Ordering::Relaxed);
        }
    }

    status.types_done.store(true, Ordering::Relaxed);
    Ok(buckets)
}

fn start_sync(app: &mut App) {
    let src = &app.buckets[app.source_idx];
    let dst = &app.buckets[app.dest_idx];

    let src_endpoint = format!("https://{}.digitaloceanspaces.com", src.region);
    let dst_endpoint = format!("https://{}.digitaloceanspaces.com", dst.region);

    let config = SyncConfig {
        source: EndpointConfig {
            endpoint: src_endpoint,
            bucket: src.name.clone(),
            access_key: app.spaces_ak.clone(),
            secret_key: app.spaces_sk.clone(),
            region: src.region.clone(),
        },
        dest: EndpointConfig {
            endpoint: dst_endpoint,
            bucket: dst.name.clone(),
            access_key: app.spaces_ak.clone(),
            secret_key: app.spaces_sk.clone(),
            region: dst.region.clone(),
        },
        concurrency: app.concurrency,
        verify_only: false,
        prefix: None,
        data_dir: app.data_dir.clone(),
        export_format: Some("csv".to_string()),
        export_path: Some(format!("{}/sync-inventory.csv", app.data_dir)),
    };

    let progress = Arc::new(SyncProgress::default());
    app.progress = Some(progress.clone());
    app.sync_start = Some(Instant::now());
    app.last_check = Instant::now();
    app.last_bytes = 0;
    app.screen = Screen::Syncing;
    app.step = 5;

    tokio::spawn(async move {
        if let Err(e) = crate::sync::run_sync(config, progress).await {
            tracing::error!(error = %e, "sync failed");
        }
    });
}

// ─── Drawing ───

fn draw(f: &mut Frame, app: &App) {
    let area = f.area();

    // Main border
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan))
        .title_top(Line::from(" do-cold-sync v0.1.0 ").centered().fg(Color::Cyan))
        .title_bottom(Line::from(" Ctrl+C: quit ").right_aligned().fg(Color::White));

    let inner = block.inner(area);
    f.render_widget(block, area);

    match app.screen {
        Screen::Welcome => draw_welcome(f, inner),
        Screen::ApiKey => draw_api_key(f, app, inner),
        Screen::Connecting => draw_connecting(f, app, inner),
        Screen::SelectSource => draw_bucket_list(f, app, inner, "SOURCE", app.source_idx, false),
        Screen::SelectDest => draw_bucket_list(f, app, inner, "DESTINATION", app.dest_idx, true),
        Screen::Confirm => draw_confirm(f, app, inner),
        Screen::Syncing => draw_syncing(f, app, inner),
        Screen::Done => draw_done(f, app, inner),
    }
}

fn draw_welcome(f: &mut Frame, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(6),
        Constraint::Length(2),
        Constraint::Length(3),
        Constraint::Length(2),
        Constraint::Min(0),
    ]).margin(1).split(area);

    let logo = Paragraph::new(LOGO)
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center);
    f.render_widget(logo, chunks[0]);

    f.render_widget(
        Paragraph::new("Migrate DigitalOcean Spaces to Cold Storage")
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        chunks[1],
    );

    f.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled("Streaming transfers", Style::default().fg(Color::Green)),
                Span::styled(" • ", Style::default().fg(Color::White)),
                Span::styled("Checkpoint resume", Style::default().fg(Color::Green)),
                Span::styled(" • ", Style::default().fg(Color::White)),
                Span::styled("Built-in verification", Style::default().fg(Color::Green)),
            ]).centered(),
        ]),
        chunks[2],
    );

    f.render_widget(
        Paragraph::new("Press Enter to start")
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::Yellow)),
        chunks[3],
    );
}

fn draw_api_key(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Min(0),
    ]).margin(1).split(area);

    draw_step_indicator(f, chunks[0], 1, "DigitalOcean API Key");

    f.render_widget(
        Paragraph::new("  Paste your DO API key (Personal Access Token):").fg(Color::White),
        chunks[1],
    );

    let masked = if app.api_key.len() > 8 {
        format!("{}...{}", &app.api_key[..8], &app.api_key[app.api_key.len()-4..])
    } else if !app.api_key.is_empty() {
        format!("{}...", &app.api_key[..app.api_key.len().min(8)])
    } else {
        String::new()
    };

    let input = Paragraph::new(format!("  {masked}"))
        .block(Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Yellow))
            .title(" API Key "));
    f.render_widget(input, chunks[2]);

    if let Some(ref err) = app.error {
        f.render_widget(
            Paragraph::new(format!("  {err}")).style(Style::default().fg(Color::Red)),
            chunks[3],
        );
    }

    f.render_widget(
        Paragraph::new("  Enter: connect  |  Esc: back").fg(Color::Gray),
        chunks[4],
    );
}

fn draw_connecting(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Min(0),
    ]).margin(1).split(area);

    draw_step_indicator(f, chunks[0], 1, "Connecting");

    let s = &app.connect_status;
    let check = |done: bool| if done { "  ✓ " } else { "  ○ " };
    let style = |done: bool, active: bool| {
        if done { Style::default().fg(Color::Green) }
        else if active { Style::default().fg(Color::Yellow) }
        else { Style::default().fg(Color::Gray) }
    };

    let keys_cleaned = s.keys_cleaned.load(Ordering::Relaxed);
    let key_created = s.key_created.load(Ordering::Relaxed);
    let buckets_listed = s.buckets_listed.load(Ordering::Relaxed);
    let types_done = s.types_done.load(Ordering::Relaxed);
    let checked = s.buckets_checked.load(Ordering::Relaxed);
    let total = s.buckets_total.load(Ordering::Relaxed);

    f.render_widget(Paragraph::new(Line::from(vec![
        Span::styled(check(keys_cleaned), style(keys_cleaned, s.cleaning_keys.load(Ordering::Relaxed))),
        Span::styled("Cleaning up old access keys", style(keys_cleaned, s.cleaning_keys.load(Ordering::Relaxed))),
    ])), chunks[1]);

    f.render_widget(Paragraph::new(Line::from(vec![
        Span::styled(check(key_created), style(key_created, s.creating_key.load(Ordering::Relaxed))),
        Span::styled("Creating Spaces access key", style(key_created, s.creating_key.load(Ordering::Relaxed))),
    ])), chunks[2]);

    f.render_widget(Paragraph::new(Line::from(vec![
        Span::styled(check(buckets_listed), style(buckets_listed, s.listing_buckets.load(Ordering::Relaxed))),
        Span::styled("Listing buckets", style(buckets_listed, s.listing_buckets.load(Ordering::Relaxed))),
    ])), chunks[3]);

    let type_label = if total > 0 {
        format!("Detecting storage tiers ({checked}/{total})")
    } else {
        "Detecting storage tiers".to_string()
    };
    f.render_widget(Paragraph::new(Line::from(vec![
        Span::styled(check(types_done), style(types_done, s.checking_types.load(Ordering::Relaxed))),
        Span::styled(type_label, style(types_done, s.checking_types.load(Ordering::Relaxed))),
    ])), chunks[4]);

    f.render_widget(
        Paragraph::new("  This may take a few seconds").fg(Color::Gray),
        chunks[5],
    );
}

fn draw_bucket_list(f: &mut Frame, app: &App, area: Rect, label: &str, selected: usize, show_cold: bool) {
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Min(5),
        Constraint::Length(1),
    ]).margin(1).split(area);

    let step = if show_cold { 3 } else { 2 };
    draw_step_indicator(f, chunks[0], step, &format!("Select {label} Bucket"));

    let subtitle = if show_cold {
        "  Select your Cold Storage destination:"
    } else {
        "  Select the standard Spaces bucket to migrate from:"
    };
    f.render_widget(Paragraph::new(subtitle).fg(Color::White), chunks[1]);

    // Filter buckets: source = standard only, dest = cold only
    let filtered: Vec<(usize, &BucketInfo)> = app.buckets.iter().enumerate()
        .filter(|(_, b)| b.is_cold == show_cold)
        .collect();

    if filtered.is_empty() {
        let msg = if show_cold {
            "  No Cold Storage buckets found. Create one in the DO Console first."
        } else {
            "  No standard Spaces buckets found."
        };
        f.render_widget(
            Paragraph::new(msg).fg(Color::Red)
                .block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded)
                    .title(format!(" {label} Buckets "))),
            chunks[2],
        );
    } else {
        let type_color = if show_cold { Color::Blue } else { Color::Green };
        let type_label = if show_cold { " COLD" } else { " STD" };

        let items: Vec<ListItem> = filtered.iter().map(|(i, b)| {
            let is_selected = *i == selected;
            let size_str = humansize::format_size(b.bytes_used, humansize::BINARY);
            let stats = format!("  {} objects, {}", b.object_count, size_str);
            let line = Line::from(vec![
                Span::raw(if is_selected { " ▸ " } else { "   " }),
                Span::styled(&b.name, if is_selected {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                }),
                Span::styled(type_label, Style::default().fg(type_color)),
                Span::styled(format!("  ({})", b.region), Style::default().fg(Color::Gray)),
                Span::styled(stats, Style::default().fg(Color::Gray)),
            ]);
            ListItem::new(line)
        }).collect();

        let list = List::new(items)
            .block(Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title(format!(" {label} Buckets ")));
        f.render_widget(list, chunks[2]);
    }

    f.render_widget(
        Paragraph::new("  ↑↓: navigate  |  Enter: select  |  'r': refresh  |  Esc: back").fg(Color::White),
        chunks[3],
    );
}

fn draw_confirm(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Length(8),
        Constraint::Length(2),
        Constraint::Min(0),
    ]).margin(1).split(area);

    draw_step_indicator(f, chunks[0], 4, "Confirm Sync");

    let src = &app.buckets[app.source_idx];
    let dst = &app.buckets[app.dest_idx];

    let details = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("    Source:  ", Style::default().fg(Color::Gray)),
            Span::styled(&src.name, Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled(format!("  ({})", src.region), Style::default().fg(Color::Gray)),
        ]),
        Line::from(vec![
            Span::styled("    Dest:    ", Style::default().fg(Color::Gray)),
            Span::styled(&dst.name, Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled(format!("  ({})", dst.region), Style::default().fg(Color::Gray)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("    Workers: ", Style::default().fg(Color::Gray)),
            Span::styled(app.concurrency.to_string(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled("  (+/- to adjust)", Style::default().fg(Color::Gray)),
        ]),
    ];

    f.render_widget(
        Paragraph::new(details)
            .block(Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)),
        chunks[2],
    );

    f.render_widget(
        Paragraph::new("  Enter/y: start sync  |  Esc/n: go back").fg(Color::Yellow),
        chunks[3],
    );
}

fn draw_syncing(f: &mut Frame, app: &App, area: Rect) {
    let Some(p) = &app.progress else { return };

    let total = p.total_source_objects.load(Ordering::Relaxed);
    let synced = p.objects_synced.load(Ordering::Relaxed);
    let skipped = p.objects_skipped.load(Ordering::Relaxed);
    let failed = p.objects_failed.load(Ordering::Relaxed);
    let verified = p.objects_verified.load(Ordering::Relaxed);
    let bytes = p.bytes_synced.load(Ordering::Relaxed);
    let phase = p.phase.lock().map(|v| v.clone()).unwrap_or_default();
    let current = p.current_key.lock().map(|v| v.clone()).unwrap_or_default();
    let done_count = synced + skipped;
    let pct = if total > 0 { (done_count as f64 / total as f64 * 100.0) as u16 } else { 0 };

    let elapsed = app.sync_start.map(|s| s.elapsed()).unwrap_or_default();
    let speed_str = if app.current_speed > 0.0 {
        format!("{}/s", humansize::format_size(app.current_speed as u64, humansize::BINARY))
    } else {
        "calculating...".to_string()
    };

    let eta = if app.current_speed > 1.0 && total > done_count {
        let remaining_estimate = ((total - done_count) as f64 / done_count as f64) * elapsed.as_secs_f64();
        if remaining_estimate < 60.0 { format!("~{}s", remaining_estimate as u64) }
        else if remaining_estimate < 3600.0 { format!("~{}m", (remaining_estimate / 60.0) as u64) }
        else { format!("~{}h", (remaining_estimate / 3600.0) as u64) }
    } else {
        "—".to_string()
    };

    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(1),
        Constraint::Length(8),
        Constraint::Length(2),
        Constraint::Min(0),
    ]).margin(1).split(area);

    draw_step_indicator(f, chunks[0], 5, &format!("Syncing — {phase}"));

    f.render_widget(
        Paragraph::new(format!("  {} / {} objects  •  {}  •  {}  •  ETA: {}",
            done_count, total,
            humansize::format_size(bytes, humansize::BINARY),
            speed_str, eta,
        )).fg(Color::White),
        chunks[1],
    );

    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded))
        .gauge_style(Style::default().fg(Color::Green).bg(Color::Gray))
        .percent(pct.min(100))
        .label(format!("{pct}%"));
    f.render_widget(gauge, chunks[2]);

    let elapsed_str = format!("{}:{:02}", elapsed.as_secs() / 60, elapsed.as_secs() % 60);

    let active = p.active_transfers.load(Ordering::Relaxed);

    let stats = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("    Synced:    ", Style::default().fg(Color::Gray)),
            Span::styled(synced.to_string(), Style::default().fg(Color::Green)),
            Span::styled("    Active:  ", Style::default().fg(Color::Gray)),
            Span::styled(active.to_string(), Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("    Skipped:   ", Style::default().fg(Color::Gray)),
            Span::styled(skipped.to_string(), Style::default().fg(Color::Blue)),
        ]),
        Line::from(vec![
            Span::styled("    Failed:    ", Style::default().fg(Color::Gray)),
            Span::styled(failed.to_string(), if failed > 0 { Style::default().fg(Color::Red) } else { Style::default() }),
        ]),
        Line::from(vec![
            Span::styled("    Verified:  ", Style::default().fg(Color::Gray)),
            Span::styled(verified.to_string(), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled("    Elapsed:   ", Style::default().fg(Color::Gray)),
            Span::raw(elapsed_str),
        ]),
    ]).block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).title(" Stats "));
    f.render_widget(stats, chunks[4]);

    let key_display = if current.len() > 70 {
        format!("...{}", &current[current.len()-67..])
    } else { current };
    f.render_widget(
        Paragraph::new(format!("  {key_display}")).fg(Color::Gray),
        chunks[5],
    );
}

fn draw_done(f: &mut Frame, app: &App, area: Rect) {
    let Some(p) = &app.progress else { return };

    let total = p.total_source_objects.load(Ordering::Relaxed);
    let synced = p.objects_synced.load(Ordering::Relaxed);
    let skipped = p.objects_skipped.load(Ordering::Relaxed);
    let failed = p.objects_failed.load(Ordering::Relaxed);
    let verified = p.objects_verified.load(Ordering::Relaxed);
    let missing = p.objects_missing.load(Ordering::Relaxed);
    let bytes = p.bytes_synced.load(Ordering::Relaxed);

    let repaired = missing.min(synced); // missing objects that were successfully re-synced
    let unresolved = missing.saturating_sub(repaired);
    let ok = failed == 0 && unresolved == 0;
    let color = if ok { Color::Green } else { Color::Red };
    let icon = if ok { "✓" } else { "✗" };
    let status = if ok { "SYNC COMPLETE" } else { "COMPLETED WITH ERRORS" };
    let elapsed = app.sync_elapsed.map(|e| {
        format!("{}:{:02}", e.as_secs() / 60, e.as_secs() % 60)
    }).unwrap_or_default();

    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(10),
        Constraint::Length(3),
        Constraint::Min(0),
    ]).margin(1).split(area);

    f.render_widget(
        Paragraph::new(format!("  {icon} {status}"))
            .style(Style::default().fg(color).add_modifier(Modifier::BOLD)),
        chunks[0],
    );

    let results = Paragraph::new(vec![
        Line::from(format!("    Total objects:  {total}")),
        Line::from(format!("    Synced:         {synced}")),
        Line::from(format!("    Skipped:        {skipped}")),
        Line::from(format!("    Failed:         {failed}")),
        Line::from(format!("    Verified:       {verified}")),
        Line::from(format!("    Missing:        {missing}")),
        Line::from(format!("    Bytes synced:   {}", humansize::format_size(bytes, humansize::BINARY))),
        Line::from(format!("    Duration:       {elapsed}")),
    ]).block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).border_style(Style::default().fg(color)).title(" Results "));
    f.render_widget(results, chunks[1]);

    let mut help_lines = vec![
        Line::from(vec![
            Span::styled("  'e'", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled(": export inventory  |  ", Style::default().fg(Color::White)),
            Span::styled("'b'", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled(": sync another bucket  |  ", Style::default().fg(Color::White)),
            Span::styled("'q'", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled(": exit", Style::default().fg(Color::White)),
        ]),
    ];
    if let Some(ref msg) = app.error {
        help_lines.push(Line::from(Span::styled(format!("  {msg}"), Style::default().fg(Color::Green))));
    }
    f.render_widget(Paragraph::new(help_lines), chunks[2]);
}

fn draw_step_indicator(f: &mut Frame, area: Rect, current: u8, label: &str) {
    let steps = ["API Key", "Source", "Dest", "Confirm", "Sync"];
    let mut spans = vec![Span::raw("  ")];

    for (i, step) in steps.iter().enumerate() {
        let num = (i + 1) as u8;
        let style = match num.cmp(&current) {
            std::cmp::Ordering::Equal => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            std::cmp::Ordering::Less => Style::default().fg(Color::Green),
            std::cmp::Ordering::Greater => Style::default().fg(Color::Gray),
        };

        let marker = if num < current { "✓" } else { &format!("{num}") };
        spans.push(Span::styled(format!("{marker} {step} "), style));

        if i < steps.len() - 1 {
            spans.push(Span::styled("→ ", Style::default().fg(Color::Gray)));
        }
    }

    spans.push(Span::raw("   "));
    spans.push(Span::styled(label, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)));

    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

async fn cleanup_key(api_key: &str, spaces_ak: &str) {
    let client = reqwest::Client::new();
    let _ = client
        .delete(format!("https://api.digitalocean.com/v2/spaces/keys/{spaces_ak}"))
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await;
}

async fn cleanup_old_keys(api_key: &str) {
    let client = reqwest::Client::new();
    let resp = client
        .get("https://api.digitalocean.com/v2/spaces/keys")
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await;

    if let Ok(resp) = resp {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(keys) = data["keys"].as_array() {
                for key in keys {
                    let name = key["name"].as_str().unwrap_or("");
                    let ak = key["access_key"].as_str().unwrap_or("");
                    if name == "do-cold-sync" && !ak.is_empty() {
                        let _ = client
                            .delete(format!("https://api.digitalocean.com/v2/spaces/keys/{ak}"))
                            .header("Authorization", format!("Bearer {api_key}"))
                            .send()
                            .await;
                    }
                }
            }
        }
    }
}
