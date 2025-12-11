use crate::config::HostConfig;
use crate::protocol::*;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use walkdir::WalkDir;
use zip::write::FileOptions;

pub async fn run_server(name: &str, cfg: HostConfig) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("binding to {}", cfg.listen))?;
    println!("[{name}] listening on {}", cfg.listen);

    loop {
        let (socket, addr) = listener.accept().await?;
        let cfg_clone = cfg.clone();
        let name = name.to_string();
        tokio::spawn(async move {
            if let Err(err) = handle_client(socket, cfg_clone).await {
                eprintln!("[{name}] client {addr} error: {err:?}");
            }
        });
    }
}

async fn handle_client(socket: tokio::net::TcpStream, cfg: HostConfig) -> Result<()> {
    let mut framed = Framed::new(
        socket,
        LengthDelimitedCodec::builder()
            .max_frame_length(FRAME_LIMIT)
            .new_codec(),
    );

    let Some(frame) = framed.next().await.transpose()? else { return Ok(()); };
    let msg: ClientMessage = bincode::deserialize(&frame)?;
    let ClientMessage::Auth { token } = msg else {
        return Err(anyhow!("expected auth first"));
    };

    let expected = fs::read_to_string(&cfg.token_file)
        .with_context(|| format!("reading token file {}", cfg.token_file.display()))?;
    let expected = expected.trim();
    if token.trim() != expected {
        send_message(&mut framed, ServerMessage::Error("auth failed".into())).await?;
        return Err(anyhow!("auth failed"));
    }

    send_message(&mut framed, ServerMessage::Authed).await?;

    while let Some(frame) = framed.next().await.transpose()? {
        let msg: ClientMessage = bincode::deserialize(&frame)?;
        match msg {
            ClientMessage::List { kind } => {
                let entries = list_entries(&cfg, kind)?;
                send_message(&mut framed, ServerMessage::ListResponse { entries }).await?;
            }
            ClientMessage::RequestDownload { entries, mode } => {
                handle_download(&cfg, entries, mode, &mut framed).await?;
            }
            ClientMessage::Ping => {
                // no-op
            }
            ClientMessage::Auth { .. } => {
                send_message(&mut framed, ServerMessage::Error("already authed".into())).await?;
            }
        }
    }

    Ok(())
}

async fn handle_download(
    cfg: &HostConfig,
    entries: Vec<RemoteEntry>,
    mode: DownloadMode,
    framed: &mut Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
) -> Result<()> {
    if entries.is_empty() {
        send_message(framed, ServerMessage::Error("no entries provided".into())).await?;
        return Ok(());
    }

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ProgressUpdate>();

    let job = DownloadJob::build(cfg, &entries, mode.clone())?;

    let zip_start = std::time::Instant::now();
    let mut blocking = spawn_blocking(move || job.materialize(progress_tx));

    let mut zip_progress_done = false;

    let built = loop {
        tokio::select! {
            maybe = progress_rx.recv(), if !zip_progress_done => {
                if let Some(p) = maybe {
                    send_message(framed, ServerMessage::Progress(p)).await?;
                } else {
                    zip_progress_done = true;
                }
            }
            join_res = &mut blocking => {
                let built = join_res??;
                break built;
            }
        }
    };
    let zip_ms = zip_start.elapsed().as_millis();

    send_message(
        framed,
        ServerMessage::Progress(ProgressUpdate {
            phase: ProgressPhase::Sending,
            done: 0,
            total: built.size,
            label: built.label.clone(),
        }),
    )
    .await?;

    let mut file = File::open(&built.path)?;
    let mut sent = 0u64;
    let mut buf = vec![0u8; CHUNK];
    let send_start = std::time::Instant::now();
    let mut read_total = 0u128;
    let mut send_total = 0u128;
    let mut read_max = 0u128;
    let mut send_max = 0u128;
    let mut chunks = 0u64;
    loop {
        let t0 = std::time::Instant::now();
        let n = file.read(&mut buf)?;
        let read_ms = t0.elapsed().as_millis();
        read_total += read_ms;
        if read_ms > read_max {
            read_max = read_ms;
        }
        if n == 0 {
            break;
        }
        sent += n as u64;
        let t1 = std::time::Instant::now();
        send_message(framed, ServerMessage::DataChunk(Bytes::copy_from_slice(&buf[..n]))).await?;
        let send_ms = t1.elapsed().as_millis();
        send_total += send_ms;
        if send_ms > send_max {
            send_max = send_ms;
        }
        chunks += 1;
        if sent % (CHUNK as u64) == 0 || sent == built.size {
            send_message(
                framed,
                ServerMessage::Progress(ProgressUpdate {
                    phase: ProgressPhase::Sending,
                    done: sent.min(built.size),
                    total: built.size,
                    label: built.label.clone(),
                }),
            )
            .await?;
        }
    }

    send_message(
        framed,
        ServerMessage::Progress(ProgressUpdate {
            phase: ProgressPhase::Completed,
            done: built.size,
            total: built.size,
            label: built.label.clone(),
        }),
    )
    .await?;
    let send_ms = send_start.elapsed().as_millis();
    let secs = (send_ms as f64 / 1000.0).max(0.001);
    let mbps = (built.size as f64 / (1024.0 * 1024.0)) / secs;
    send_message(
        framed,
        ServerMessage::Stats {
            zip_ms,
            send_wall_ms: send_ms,
            bytes: built.size,
            avg_mb_s: mbps,
            read_ms: read_total,
            send_ms: send_total,
            max_read_ms: read_max,
            max_send_ms: send_max,
            chunks,
        },
    )
    .await
    .ok();
    println!(
        "[stats] {} bytes zip={}ms send_wall={}ms read_total={}ms send_total={}ms max_read={}ms max_send={}ms chunks={} avg={:.2} MB/s",
        built.size, zip_ms, send_ms, read_total, send_total, read_max, send_max, chunks, mbps
    );
    send_message(framed, ServerMessage::Done).await?;
    built.cleanup();
    Ok(())
}

enum DownloadJob {
    SingleFile {
        path: PathBuf,
        label: String,
    },
    Zip {
        label: String,
        entries: Vec<ZipEntry>,
    },
}

struct ZipEntry {
    absolute: PathBuf,
    archive_path: String,
    is_dir: bool,
    size: u64,
}

struct BuiltDownload {
    path: PathBuf,
    size: u64,
    label: String,
    tmp: Option<tempfile::NamedTempFile>,
}

impl BuiltDownload {
    fn cleanup(self) {
        drop(self.tmp);
    }
}

impl DownloadJob {
    fn build(cfg: &HostConfig, entries: &[RemoteEntry], mode: DownloadMode) -> Result<Self> {
        match mode {
            DownloadMode::SchematicSingle => {
                if entries.len() != 1 {
                    return Err(anyhow!("schematic single requires exactly one entry"));
                }
                let entry = &entries[0];
                if entry.kind != EntryKind::SchematicFile {
                    return Err(anyhow!("schematic single expects a file"));
                }
                let abs = resolve_entry(cfg, entry)?;
                Ok(DownloadJob::SingleFile {
                    path: abs,
                    label: entry.path.clone(),
                })
            }
            DownloadMode::SchematicBundle => {
                let zentries = prepare_zip_entries(cfg, entries)?;
                Ok(DownloadJob::Zip {
                    label: "schematics.zip".into(),
                    entries: zentries,
                })
            }
            DownloadMode::WorldZip | DownloadMode::RegionZip => {
                let entry = entries.get(0).ok_or_else(|| anyhow!("world entry missing"))?;
                if entry.kind != EntryKind::World {
                    return Err(anyhow!("world download requires a world entry"));
                }
                let abs = resolve_entry(cfg, entry)?;
                let zentries = if matches!(mode, DownloadMode::RegionZip) {
                    let region = abs.join("region");
                    if !region.exists() {
                        return Err(anyhow!("region folder missing for {}", entry.path));
                    }
                    gather_zip_entries(&region, &region, true)?
                } else {
                    gather_zip_entries(&abs, &abs, true)?
                };
                let label = if matches!(mode, DownloadMode::RegionZip) {
                    format!("{}-region.zip", file_stem(&entry.path))
                } else {
                    format!("{}-world.zip", file_stem(&entry.path))
                };
                Ok(DownloadJob::Zip { label, entries: zentries })
            }
        }
    }

    fn materialize(self, progress: mpsc::UnboundedSender<ProgressUpdate>) -> Result<BuiltDownload> {
        match self {
            DownloadJob::SingleFile { path, label } => {
                let size = fs::metadata(&path)?.len();
                Ok(BuiltDownload {
                    path,
                    size,
                    label,
                    tmp: None,
                })
            }
            DownloadJob::Zip { label, entries } => {
                let tmp = tempfile::NamedTempFile::new()?;
                let path = tmp.path().to_path_buf();
                let total: u64 = entries.iter().map(|e| e.size).sum();
                progress
                    .send(ProgressUpdate {
                        phase: ProgressPhase::Zipping,
                        done: 0,
                        total,
                        label: label.clone(),
                    })
                    .ok();
                zip_entries(&path, &entries, total, &progress)?;
                let size = fs::metadata(&path)?.len();
                Ok(BuiltDownload {
                    path,
                    size,
                    label,
                    tmp: Some(tmp),
                })
            }
        }
    }
}

fn list_entries(cfg: &HostConfig, kind: ListKind) -> Result<Vec<RemoteEntry>> {
    let mut out = Vec::new();
    let roots: Vec<(String, PathBuf)> = match kind {
        ListKind::Worlds => cfg
            .worlds
            .iter()
            .enumerate()
            .map(|(i, p)| (format!("worlds#{i}"), p.clone()))
            .collect(),
        ListKind::Schematics => cfg
            .schematics
            .iter()
            .enumerate()
            .map(|(i, p)| (format!("schematics#{i}"), p.clone()))
            .collect(),
    };

    for (label, root) in roots {
        match kind {
            ListKind::Worlds => {
                for entry in WalkDir::new(&root)
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|e| e.depth() > 0 && e.file_type().is_dir())
                {
                    let world_path = entry.path();
                    let region_dir = world_path.join("region");
                    if !region_dir.is_dir() {
                        continue;
                    }
                    let rel = world_path
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.push(RemoteEntry {
                        root: label.clone(),
                        path: rel,
                        kind: EntryKind::World,
                        size: dir_size(world_path)?,
                    });
                }
            }
            ListKind::Schematics => {
                for entry in WalkDir::new(&root)
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|e| e.depth() > 0)
                {
                    let rel = entry
                        .path()
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/");
                    let md = entry.metadata()?;
                    if md.is_dir() {
                        out.push(RemoteEntry {
                            root: label.clone(),
                            path: rel.clone(),
                            kind: EntryKind::SchematicDir,
                            size: if md.is_dir() { dir_size(entry.path())? } else { md.len() },
                        });
                    } else if md.is_file() {
                        out.push(RemoteEntry {
                            root: label.clone(),
                            path: rel.clone(),
                            kind: EntryKind::SchematicFile,
                            size: md.len(),
                        });
                    }
                }
            }
        }
    }

    Ok(out)
}

fn resolve_entry(cfg: &HostConfig, entry: &RemoteEntry) -> Result<PathBuf> {
    let root = find_root(cfg, &entry.root)?;
    let abs = root.join(&entry.path);
    let canon_root = root
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", root.display()))?;
    let canon_path = abs
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", abs.display()))?;
    if !canon_path.starts_with(&canon_root) {
        return Err(anyhow!("entry escapes root"));
    }
    Ok(canon_path)
}

fn find_root(cfg: &HostConfig, label: &str) -> Result<PathBuf> {
    let mut map = HashMap::new();
    for (i, p) in cfg.worlds.iter().enumerate() {
        map.insert(format!("worlds#{i}"), p.clone());
    }
    for (i, p) in cfg.schematics.iter().enumerate() {
        map.insert(format!("schematics#{i}"), p.clone());
    }
    map.remove(label)
        .ok_or_else(|| anyhow!("unknown root label {label}"))
}

fn prepare_zip_entries(cfg: &HostConfig, entries: &[RemoteEntry]) -> Result<Vec<ZipEntry>> {
    let mut out = Vec::new();
    for entry in entries {
        let abs = resolve_entry(cfg, entry)?;
        match entry.kind {
            EntryKind::SchematicFile | EntryKind::World => {
                let size = fs::metadata(&abs)?.len();
                out.push(ZipEntry {
                    absolute: abs.clone(),
                    archive_path: entry.path.replace('\\', "/"),
                    is_dir: false,
                    size,
                });
            }
            EntryKind::SchematicDir => {
                let mut nested = gather_zip_entries(&abs, &abs, false)?;
                for n in nested.iter_mut() {
                    n.archive_path = format!(
                        "{}/{}",
                        entry.path.replace('\\', "/"),
                        n.archive_path.clone()
                    );
                }
                out.extend(nested);
            }
        }
    }
    Ok(out)
}

fn gather_zip_entries(root: &Path, path: &Path, include_root: bool) -> Result<Vec<ZipEntry>> {
    let mut out = Vec::new();
    for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
        let md = entry.metadata()?;
        let rel = entry
            .path()
            .strip_prefix(root)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .replace('\\', "/");
        let archive_path = if include_root {
            let name = root
                .file_name()
                .map(|s| s.to_string_lossy())
                .unwrap_or_else(|| "".into());
            if rel.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", name, rel)
            }
        } else {
            rel
        };
        if archive_path.is_empty() {
            continue;
        }
        if md.is_dir() {
            out.push(ZipEntry {
                absolute: entry.path().to_path_buf(),
                archive_path: ensure_trailing_slash(&archive_path),
                is_dir: true,
                size: 0,
            });
        } else {
            out.push(ZipEntry {
                absolute: entry.path().to_path_buf(),
                archive_path,
                is_dir: false,
                size: md.len(),
            });
        }
    }
    Ok(out)
}

fn ensure_trailing_slash(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    if s.ends_with('/') {
        s.to_string()
    } else {
        format!("{s}/")
    }
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn zip_entries(
    output: &Path,
    entries: &[ZipEntry],
    total: u64,
    progress: &mpsc::UnboundedSender<ProgressUpdate>,
) -> Result<()> {
    let file = File::create(output)?;
    let mut writer = zip::ZipWriter::new(file);
    let options_store = FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let options_deflate = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let mut done = 0u64;
    let mut buffer = vec![0u8; CHUNK];
    for entry in entries {
        let opts = if entry.archive_path.ends_with(".schematic") || entry.archive_path.ends_with(".schem") {
            options_deflate
        } else {
            options_store
        };
        if entry.is_dir {
            writer.add_directory(entry.archive_path.clone(), opts)?;
            continue;
        }
        writer.start_file(entry.archive_path.clone(), opts)?;
        let mut f = File::open(&entry.absolute)?;
        loop {
            let n = f.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buffer[..n])?;
            done += n as u64;
            if done % (CHUNK as u64) == 0 || done == total {
                progress
                    .send(ProgressUpdate {
                        phase: ProgressPhase::Zipping,
                        done: done.min(total),
                        total,
                        label: entry.archive_path.clone(),
                    })
                    .ok();
            }
        }
    }
    writer.finish()?;
    Ok(())
}

async fn send_message(
    framed: &mut Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    msg: ServerMessage,
) -> Result<()> {
    let data = bincode::serialize(&msg)?;
    framed.send(Bytes::from(data)).await?;
    Ok(())
}

fn file_stem(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("world")
        .to_string()
}
