use crate::client::{RemoteClient, TransferStats};
use crate::protocol::*;
use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph};
use ratatui::Terminal;
use std::collections::{HashSet, VecDeque};
use std::collections::HashMap;
use std::fs;
use std::io::stdout;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub async fn run_tui(
    server_name: String,
    addr: String,
    token_file: PathBuf,
    output_dir: PathBuf,
    servers: std::collections::HashMap<String, crate::config::HostConfig>,
    streams: u32,
) -> Result<()> {
    let token = fs::read_to_string(&token_file)
        .with_context(|| format!("reading token {}", token_file.display()))?;

    let _ = fs::create_dir_all(&output_dir);

    let mut client = Arc::new(tokio::sync::Mutex::new(
        RemoteClient::connect(&addr, token.clone()).await?,
    ));

    let worlds = {
        let mut guard = client.lock().await;
        guard.list(ListKind::Worlds).await?
    };
    let schematics = {
        let mut guard = client.lock().await;
        guard.list(ListKind::Schematics).await?
    };

    let mut app = App::new(server_name, addr, output_dir, worlds, schematics, servers, streams);

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event>();
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ProgressUpdate>();
    let (stats_tx, mut stats_rx) = mpsc::unbounded_channel::<TransferStats>();

    let running = Arc::new(AtomicBool::new(true));
    let running_flag = running.clone();
    tokio::task::spawn_blocking(move || {
        while running_flag.load(Ordering::Relaxed) {
            if let Ok(true) = event::poll(std::time::Duration::from_millis(100)) {
                if let Ok(ev) = event::read() {
                    let _ = event_tx.send(ev);
                }
            }
        }
    });

    enable_raw_mode()?;
    execute!(stdout(), terminal::EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut inflight: Option<JoinHandle<()>> = None;
    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        tokio::select! {
            Some(ev) = event_rx.recv() => {
                if handle_event(ev, &mut app) {
                    running.store(false, Ordering::Relaxed);
                    break;
                }
                if let Some(next_server) = app.pending_switch.take() {
                    if let Some(host) = app.servers.get(&next_server).cloned() {
                        let token = fs::read_to_string(&host.token_file)
                            .with_context(|| format!("reading token {}", host.token_file.display()))?;
                        let new_client = Arc::new(tokio::sync::Mutex::new(
                            RemoteClient::connect(&host.listen, token).await?,
                        ));
                        let worlds = {
                            let mut guard = new_client.lock().await;
                            guard.list(ListKind::Worlds).await?
                        };
                        let schematics = {
                            let mut guard = new_client.lock().await;
                            guard.list(ListKind::Schematics).await?
                        };
                        app.server_name = next_server.clone();
                        app.addr = host.listen.clone();
                        app.worlds = worlds;
                        app.schematics = schematics;
                        app.selected.clear();
                        app.cursor = 0;
                        app.apply_filter();
                        app.progress = None;
                        app.status = format!("Switched to {}", next_server);
                        client = new_client;
                        continue;
                    }
                }
                if let Some(action) = app.take_action() {
                    if inflight.is_some() { continue; }
                    let entries = action.entries.clone();
                    let mode = action.mode;
                    let target = app.build_target_path(&entries, mode.clone());
                    let target_for_task = target.clone();
                    let progress = progress_tx.clone();
                    let streams = app.streams;
                    let stats = stats_tx.clone();
                    let client = client.clone();
                    inflight = Some(tokio::spawn(async move {
                        let res = {
                            let mut guard = client.lock().await;
                            guard.download(entries, mode, &target_for_task, progress.clone(), stats.clone(), streams).await
                        };
                        if let Err(err) = res {
                            let _ = progress.send(ProgressUpdate{
                                phase: ProgressPhase::Completed,
                                done:0,
                                total:0,
                                label: format!("error: {err}"),
                            });
                        }
                    }));
                    app.status = format!("Downloading to {}", target.display());
                }
            }
            Some(p) = progress_rx.recv() => {
                app.progress = Some(p.clone());
                app.update_speed(&p);
                match p.phase {
                    ProgressPhase::Completed => {
                        inflight = None;
                        app.status = format!("Completed: {}", p.label);
                        app.last_progress = None;
                        app.speed = None;
                    }
                    _ => {
                        app.status = format!("{:?} {}", p.phase, p.label);
                    }
                }
            }
            Some(stats) = stats_rx.recv() => {
                app.last_stats = Some(stats);
            }
        }
    }

    disable_raw_mode()?;
    execute!(stdout(), terminal::LeaveAlternateScreen)?;
    Ok(())
}

struct App {
    server_name: String,
    addr: String,
    output_dir: PathBuf,
    current: ListKind,
    worlds: Vec<RemoteEntry>,
    schematics: Vec<RemoteEntry>,
    filtered: Vec<usize>,
    selected: HashSet<usize>,
    cursor: usize,
    search: String,
    progress: Option<ProgressUpdate>,
    status: String,
    pending_actions: VecDeque<PendingAction>,
    last_progress: Option<(Instant, u64)>,
    speed: Option<String>,
    servers: HashMap<String, crate::config::HostConfig>,
    pending_switch: Option<String>,
    last_stats: Option<TransferStats>,
    streams: u32,
}

struct PendingAction {
    entries: Vec<RemoteEntry>,
    mode: DownloadMode,
}

impl App {
    fn new(
        server_name: String,
        addr: String,
        output_dir: PathBuf,
        worlds: Vec<RemoteEntry>,
        schematics: Vec<RemoteEntry>,
        servers: HashMap<String, crate::config::HostConfig>,
        streams: u32,
    ) -> Self {
        let mut app = Self {
            server_name,
            addr,
            output_dir,
            current: ListKind::Worlds,
            worlds,
            schematics,
            filtered: Vec::new(),
            selected: HashSet::new(),
            cursor: 0,
            search: String::new(),
            progress: None,
            status: "Ready".into(),
            pending_actions: VecDeque::new(),
            last_progress: None,
            speed: None,
            servers,
            pending_switch: None,
            last_stats: None,
            streams,
        };
        app.apply_filter();
        app
    }

    fn entries(&self) -> &Vec<RemoteEntry> {
        match self.current {
            ListKind::Worlds => &self.worlds,
            ListKind::Schematics => &self.schematics,
        }
    }

    fn apply_filter(&mut self) {
        let query = self.search.to_lowercase();
        let entries = self.entries();
        let filtered: Vec<usize> = (0..entries.len())
            .filter(|&idx| {
                let entry = &entries[idx];
                query.is_empty()
                    || entry.path.to_lowercase().contains(&query)
                    || entry.root.to_lowercase().contains(&query)
            })
            .collect();
        self.filtered = filtered;
        if self.cursor >= self.filtered.len() {
            self.cursor = self.filtered.len().saturating_sub(1);
        }
    }

    fn toggle_selection(&mut self) {
        if let Some(idx) = self.filtered.get(self.cursor).cloned() {
            if self.selected.contains(&idx) {
                self.selected.remove(&idx);
            } else {
                self.selected.insert(idx);
            }
        }
    }

    fn move_cursor(&mut self, delta: isize) {
        if self.filtered.is_empty() {
            self.cursor = 0;
            return;
        }
        let len = self.filtered.len() as isize;
        let next = (self.cursor as isize + delta).clamp(0, len - 1);
        self.cursor = next as usize;
    }

    fn take_action(&mut self) -> Option<PendingAction> {
        self.pending_actions.pop_front()
    }

    fn build_target_path(&self, entries: &[RemoteEntry], mode: DownloadMode) -> PathBuf {
        let first = entries.first();
        let name = first
            .map(|e| Path::new(&e.path).file_name().and_then(|s| s.to_str()).unwrap_or("download"))
            .unwrap_or("download");
        let filename = match mode {
            DownloadMode::WorldZip => format!("{}-world.zip", name),
            DownloadMode::RegionZip => format!("{}-region.zip", name),
            DownloadMode::SchematicSingle => name.to_string(),
            DownloadMode::SchematicBundle => format!("{}-bundle.zip", name),
        };
        self.output_dir.join(filename)
    }

    fn update_speed(&mut self, p: &ProgressUpdate) {
        if p.phase != ProgressPhase::Sending {
            self.last_progress = None;
            self.speed = None;
            return;
        }
        let now = Instant::now();
        if let Some((last_t, last_done)) = self.last_progress {
            let dt = now.duration_since(last_t).as_secs_f64();
            if dt > 0.0 {
                let delta = p.done.saturating_sub(last_done) as f64;
                let bps = delta / dt;
                self.speed = Some(format_speed(bps));
            }
        }
        self.last_progress = Some((now, p.done));
    }

    fn queue_next_server(&mut self) {
        if self.servers.len() <= 1 {
            return;
        }
        let mut names: Vec<String> = self.servers.keys().cloned().collect();
        names.sort();
        if let Some(pos) = names.iter().position(|n| n == &self.server_name) {
            let next = names.get((pos + 1) % names.len()).cloned().unwrap_or_else(|| self.server_name.clone());
            if next != self.server_name {
                self.pending_switch = Some(next);
            }
        }
    }
}

fn draw_ui(f: &mut ratatui::Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5), Constraint::Length(3)].as_ref())
        .split(f.size());

    let title = Paragraph::new(format!(
        "{} @ {}  | mode: {:?}  | search: {}",
        app.server_name, app.addr, app.current, app.search
    ))
    .block(Block::default().borders(Borders::ALL).title("K2 Fast Downloader"));
    f.render_widget(title, chunks[0]);

    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .split(chunks[1]);

    let items: Vec<ListItem> = app
        .filtered
        .iter()
        .enumerate()
        .map(|(visible_idx, real_idx)| {
            let entry = &app.entries()[*real_idx];
            let selected = app.selected.contains(real_idx);
            let marker = if selected { "[x]" } else { "[ ]" };
            let line = format!(
                "{} {} ({:?}, {})",
                marker,
                entry.path,
                entry.kind,
                format_size(entry.size)
            );
            let mut item = ListItem::new(line);
            if visible_idx == app.cursor {
                item = item.style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
            }
            item
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Items"))
        .highlight_style(Style::default().bg(Color::Blue));
    f.render_widget(list, body_chunks[0]);

    let mut info_lines = vec![Line::from(format!("Status: {}", app.status))];
    if let Some(stats) = &app.last_stats {
        let origin = match stats.origin {
            crate::client::StatsOrigin::Server => "server",
            crate::client::StatsOrigin::Client => "client",
        };
        info_lines.push(Line::from(format!(
            "Last ({origin}): {:.2} MB/s | {} | zip: {} ms | send_wall: {} ms",
            stats.avg_mb_s,
            format_size(stats.bytes),
            stats.zip_ms,
            stats.send_wall_ms,
        )));
        if stats.origin == crate::client::StatsOrigin::Server {
            info_lines.push(Line::from(format!(
                "    read_total: {} ms (max {} ms) | send_total: {} ms (max {} ms) | chunks: {}",
                stats.read_ms,
                stats.max_read_ms,
                stats.send_ms,
                stats.max_send_ms,
                stats.chunks
            )));
        } else {
            info_lines.push(Line::from(format!(
                "    write_total: {} ms (max {} ms) | gap_max: {} ms",
                stats.client_write_ms,
                stats.client_max_write_ms,
                stats.client_gap_max_ms
            )));
        }
    }
    let mut gauge = None;
    if let Some(p) = &app.progress {
        let pct = if p.total > 0 {
            (p.done as f64 / p.total as f64 * 100.0).min(100.0)
        } else {
            0.0
        };
        info_lines.push(Line::from(format!(
            "{:?}: {} / {} [{}] ({:.1}%){}",
            p.phase,
            format_size(p.done),
            format_size(p.total),
            p.label,
            pct,
            app.speed
                .as_deref()
                .map(|s| format!(" | {}", s))
                .unwrap_or_default()
        )));
        if p.phase != ProgressPhase::Completed {
            gauge = Some(
                Gauge::default()
                    .gauge_style(
                        Style::default()
                            .fg(Color::Green)
                            .bg(Color::Black)
                            .add_modifier(Modifier::BOLD),
                    )
                    .ratio(if p.total > 0 {
                        (p.done as f64 / p.total as f64).min(1.0)
                    } else {
                        0.0
                    }),
            );
        }
    }

    let progress_block = Block::default().borders(Borders::ALL).title("Progress");
    let inner = progress_block.inner(body_chunks[1]);
    f.render_widget(progress_block, body_chunks[1]);

    let right_split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)].as_ref())
        .split(inner);

    let info = Paragraph::new(info_lines);
    f.render_widget(info, right_split[0]);
    if let Some(g) = gauge {
        f.render_widget(g, right_split[1]);
    }

    let footer = Paragraph::new(vec![
        Line::from("Ctrl+j/k or arrows: move  | space: select | /: search | tab: toggle worlds/schematics | Ctrl+s: next server"),
        Line::from("Actions: 1=world zip  2=region zip  3=schem file  4=schem bundle  q=quit"),
    ])
    .block(Block::default().borders(Borders::ALL).title("Keys"));
    f.render_widget(footer, chunks[2]);
}

fn handle_event(ev: Event, app: &mut App) -> bool {
    if let Event::Key(key) = ev {
        if key.kind == KeyEventKind::Press {
            match key.code {
                KeyCode::Char('q') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => return true,
                KeyCode::Esc => return true,
                KeyCode::Char('j') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    app.move_cursor(1)
                }
                KeyCode::Char('k') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    app.move_cursor(-1)
                }
                KeyCode::Down => app.move_cursor(1),
                KeyCode::Up => app.move_cursor(-1),
                KeyCode::Char(' ') => app.toggle_selection(),
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    app.queue_next_server();
                }
                KeyCode::Tab => {
                    app.current = match app.current {
                        ListKind::Worlds => ListKind::Schematics,
                        ListKind::Schematics => ListKind::Worlds,
                    };
                    app.cursor = 0;
                    app.selected.clear();
                    app.apply_filter();
                }
                KeyCode::Char('/') => {
                    app.search.clear();
                }
                KeyCode::Backspace => {
                    app.search.pop();
                    app.apply_filter();
                }
                KeyCode::Char('1') => trigger_action(app, DownloadMode::WorldZip),
                KeyCode::Char('2') => trigger_action(app, DownloadMode::RegionZip),
                KeyCode::Char('3') => trigger_action(app, DownloadMode::SchematicSingle),
                KeyCode::Char('4') => trigger_action(app, DownloadMode::SchematicBundle),
                KeyCode::Char(c) => {
                    if c.is_ascii_graphic() || c == ' ' {
                        app.search.push(c);
                        app.apply_filter();
                    }
                }
                _ => {}
            }
        }
    }
    false
}

fn trigger_action(app: &mut App, mode: DownloadMode) {
    let mut entries = Vec::new();
    for idx in &app.selected {
        if let Some(entry) = app.entries().get(*idx) {
            entries.push(entry.clone());
        }
    }
    if entries.is_empty() {
        if let Some(idx) = app.filtered.get(app.cursor) {
            if let Some(entry) = app.entries().get(*idx) {
                entries.push(entry.clone());
            }
        }
    }
    if entries.is_empty() {
        app.status = "Nothing selected".into();
        return;
    }
    match mode {
        DownloadMode::WorldZip | DownloadMode::RegionZip => {
            entries.retain(|e| e.kind == EntryKind::World);
        }
        DownloadMode::SchematicSingle => {
            entries.retain(|e| e.kind == EntryKind::SchematicFile);
            entries.truncate(1);
        }
        DownloadMode::SchematicBundle => {
            entries.retain(|e| e.kind != EntryKind::World);
        }
    }
    if entries.is_empty() {
        app.status = "Selection incompatible with action".into();
        return;
    }
    match mode {
        DownloadMode::WorldZip | DownloadMode::RegionZip => {
            let count = entries.len();
            for e in entries {
                app.pending_actions.push_back(PendingAction {
                    entries: vec![e],
                    mode: mode.clone(),
                });
            }
            app.status = format!("Queued {} world downloads", count);
        }
        _ => {
            app.pending_actions
                .push_back(PendingAction { entries, mode: mode.clone() });
            app.status = format!("Queued {:?} ({})", mode, app.pending_actions.len());
        }
    }
    app.progress = None;
    app.selected.clear();
}

fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.2} MB", b / MB)
    } else if b >= KB {
        format!("{:.2} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn format_speed(bytes_per_sec: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    if bytes_per_sec >= GB {
        format!("{:.2} GB/s", bytes_per_sec / GB)
    } else if bytes_per_sec >= MB {
        format!("{:.2} MB/s", bytes_per_sec / MB)
    } else if bytes_per_sec >= KB {
        format!("{:.2} KB/s", bytes_per_sec / KB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}
