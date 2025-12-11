use crate::protocol::*;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::path::Path;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::fs;
use tokio::io::{AsyncWriteExt, AsyncSeekExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct RemoteClient {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
    addr: String,
    token: String,
}

impl RemoteClient {
    pub async fn connect(addr: &str, token: String) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connecting to {addr}"))?;
        let mut framed = Framed::new(
            stream,
            LengthDelimitedCodec::builder()
                .max_frame_length(FRAME_LIMIT)
                .new_codec(),
        );
        send(&mut framed, ClientMessage::Auth { token: token.clone() }).await?;
        let Some(frame) = framed.next().await.transpose()? else {
            return Err(anyhow!("connection closed during auth"));
        };
        let msg: ServerMessage = bincode::deserialize(&frame)?;
        match msg {
            ServerMessage::Authed => Ok(Self { framed, addr: addr.to_string(), token }),
            ServerMessage::Error(e) => Err(anyhow!("auth failed: {e}")),
            other => Err(anyhow!("unexpected auth response: {:?}", other)),
        }
    }

    async fn connect_fresh(&self) -> Result<Self> {
        RemoteClient::connect(&self.addr, self.token.clone()).await
    }

    pub async fn list(&mut self, kind: ListKind) -> Result<Vec<RemoteEntry>> {
        send(&mut self.framed, ClientMessage::List { kind }).await?;
        let Some(frame) = self.framed.next().await.transpose()? else {
            return Err(anyhow!("connection closed while listing"));
        };
        let msg: ServerMessage = bincode::deserialize(&frame)?;
        match msg {
            ServerMessage::ListResponse { entries } => Ok(entries),
            ServerMessage::Error(e) => Err(anyhow!(e)),
            other => Err(anyhow!("unexpected list response: {:?}", other)),
        }
    }

    pub async fn download(
        &mut self,
        entries: Vec<RemoteEntry>,
        mode: DownloadMode,
        target_path: &Path,
        progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
        stats_tx: mpsc::UnboundedSender<TransferStats>,
        streams: u32,
    ) -> Result<()> {
        if streams <= 1 {
            self.download_single(entries, mode, target_path, progress_tx, stats_tx)
                .await
        } else {
            self.download_parallel(entries, mode, target_path, progress_tx, stats_tx, streams)
                .await
        }
    }

    async fn download_single(
        &mut self,
        entries: Vec<RemoteEntry>,
        mode: DownloadMode,
        target_path: &Path,
        progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
        stats_tx: mpsc::UnboundedSender<TransferStats>,
    ) -> Result<()> {
        let started = Instant::now();
        let mut received = 0u64;
        let mut write_total = 0u128;
        let mut write_max = 0u128;
        let mut gap_max = 0u128;
        let mut last_frame = Instant::now();
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await.ok();
        }
        let mut file = fs::File::create(target_path)
            .await
            .with_context(|| format!("creating {}", target_path.display()))?;

        send(
            &mut self.framed,
            ClientMessage::RequestDownload { entries, mode },
        )
        .await?;

        while let Some(frame) = self.framed.next().await.transpose()? {
            let msg: ServerMessage = bincode::deserialize(&frame)?;
            let now = Instant::now();
            let gap = now.duration_since(last_frame).as_millis();
            if gap > gap_max {
                gap_max = gap;
            }
            last_frame = now;
            match msg {
                ServerMessage::Progress(p) => {
                    let _ = progress_tx.send(p);
                }
                ServerMessage::DataChunk(chunk) => {
                    let t0 = Instant::now();
                    file.write_all(&chunk).await?;
                    received += chunk.len() as u64;
                    let w = t0.elapsed().as_millis();
                    write_total += w;
                    if w > write_max {
                        write_max = w;
                    }
                }
                ServerMessage::Stats { zip_ms, send_wall_ms, bytes, avg_mb_s, read_ms, send_ms, max_read_ms, max_send_ms, chunks } => {
                    let _ = stats_tx.send(TransferStats {
                        zip_ms,
                        send_wall_ms,
                        bytes,
                        avg_mb_s,
                        read_ms,
                        send_ms,
                        max_read_ms,
                        max_send_ms,
                        chunks,
                        origin: StatsOrigin::Server,
                        client_write_ms: 0,
                        client_max_write_ms: 0,
                        client_gap_max_ms: 0,
                    });
                }
                ServerMessage::Done => break,
                ServerMessage::Error(e) => return Err(anyhow!(e)),
                _ => {}
            }
        }

        let elapsed = started.elapsed().as_millis();
        if elapsed > 0 {
            let secs = (elapsed as f64 / 1000.0).max(0.001);
            let mbps = (received as f64 / (1024.0 * 1024.0)) / secs;
            let _ = stats_tx.send(TransferStats {
                zip_ms: 0,
                send_wall_ms: elapsed,
                bytes: received,
                avg_mb_s: mbps,
                read_ms: 0,
                send_ms: 0,
                max_read_ms: 0,
                max_send_ms: 0,
                chunks: 0,
                origin: StatsOrigin::Client,
                client_write_ms: write_total,
                client_max_write_ms: write_max,
                client_gap_max_ms: gap_max,
            });
        }

        Ok(())
    }

    async fn download_parallel(
        &mut self,
        entries: Vec<RemoteEntry>,
        mode: DownloadMode,
        target_path: &Path,
        progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
        stats_tx: mpsc::UnboundedSender<TransferStats>,
        streams: u32,
    ) -> Result<()> {
        let started = Instant::now();
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await.ok();
        }

        send(
            &mut self.framed,
            ClientMessage::RequestDownloadParallel {
                entries,
                mode,
                streams,
            },
        )
        .await?;

        let mut transfer_id = None;
        let mut size = 0u64;
        while let Some(frame) = self.framed.next().await.transpose()? {
            let msg: ServerMessage = bincode::deserialize(&frame)?;
            match msg {
                ServerMessage::TransferReady { transfer_id: tid, size: sz, .. } => {
                    transfer_id = Some(tid);
                    size = sz;
                    break;
                }
                ServerMessage::Error(e) => return Err(anyhow!(e)),
                _ => {}
            }
        }
        let tid = transfer_id.ok_or_else(|| anyhow!("no transfer id from server"))?;

        let mut out = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(target_path)
            .await
            .with_context(|| format!("opening {}", target_path.display()))?;
        out.set_len(size).await?;

        let done_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let chunk_size = (size + streams as u64 - 1) / streams as u64;
        let mut handles = Vec::new();

        for i in 0..streams {
            let start = i as u64 * chunk_size;
            if start >= size {
                continue;
            }
            let end = ((i as u64 + 1) * chunk_size).min(size);
            let len = end - start;
            let path = target_path.to_owned();
            let tid_clone = tid.clone();
            let done = done_bytes.clone();
            let progress = progress_tx.clone();
            let client = self.connect_fresh().await?;
            let handle = tokio::spawn(async move {
                download_range(
                    client,
                    tid_clone,
                    start,
                    len,
                    &path,
                    done,
                    progress,
                    size,
                )
                .await
            });
            handles.push(handle);
        }

        for h in handles {
            h.await??;
        }

        // release transfer
        send(
            &mut self.framed,
            ClientMessage::ReleaseTransfer {
                transfer_id: tid.clone(),
            },
        )
        .await?;
        while let Some(frame) = self.framed.next().await.transpose()? {
            let msg: ServerMessage = bincode::deserialize(&frame)?;
            if let ServerMessage::Done = msg {
                break;
            }
        }

        let elapsed = started.elapsed().as_millis();
        let secs = (elapsed as f64 / 1000.0).max(0.001);
        let mbps = (size as f64 / (1024.0 * 1024.0)) / secs;
        let _ = stats_tx.send(TransferStats {
            zip_ms: 0,
            send_wall_ms: elapsed,
            bytes: size,
            avg_mb_s: mbps,
            read_ms: 0,
            send_ms: 0,
            max_read_ms: 0,
            max_send_ms: 0,
            chunks: streams as u64,
            origin: StatsOrigin::Client,
            client_write_ms: 0,
            client_max_write_ms: 0,
            client_gap_max_ms: 0,
        });

        Ok(())
    }
}

async fn send(framed: &mut Framed<TcpStream, LengthDelimitedCodec>, msg: ClientMessage) -> Result<()> {
    let data = bincode::serialize(&msg)?;
    framed.send(Bytes::from(data)).await?;
    Ok(())
}

async fn download_range(
    mut client: RemoteClient,
    transfer_id: String,
    offset: u64,
    len: u64,
    target_path: &Path,
    done: std::sync::Arc<std::sync::atomic::AtomicU64>,
    progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
    total: u64,
) -> Result<()> {
    send(
        &mut client.framed,
        ClientMessage::RangeRequest {
            transfer_id,
            offset,
            len,
        },
    )
    .await?;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(target_path)
        .await
        .with_context(|| format!("opening {}", target_path.display()))?;

    while let Some(frame) = client.framed.next().await.transpose()? {
        let msg: ServerMessage = bincode::deserialize(&frame)?;
        match msg {
            ServerMessage::RangeChunk { offset: chunk_off, data } => {
                file.seek(std::io::SeekFrom::Start(chunk_off)).await?;
                file.write_all(&data).await?;
                let new_done = done.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed)
                    + data.len() as u64;
                let _ = progress_tx.send(ProgressUpdate {
                    phase: ProgressPhase::Sending,
                    done: new_done,
                    total,
                    label: "parallel".into(),
                });
            }
            ServerMessage::Done => break,
            ServerMessage::Error(e) => return Err(anyhow!(e)),
            _ => {}
        }
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq)]
pub enum StatsOrigin {
    Server,
    Client,
}

#[derive(Debug, Clone)]
pub struct TransferStats {
    pub zip_ms: u128,
    pub send_wall_ms: u128,
    pub bytes: u64,
    pub avg_mb_s: f64,
    pub read_ms: u128,
    pub send_ms: u128,
    pub max_read_ms: u128,
    pub max_send_ms: u128,
    pub chunks: u64,
    pub origin: StatsOrigin,
    pub client_write_ms: u128,
    pub client_max_write_ms: u128,
    pub client_gap_max_ms: u128,
}
