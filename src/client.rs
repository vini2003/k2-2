use crate::protocol::*;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::path::Path;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct RemoteClient {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
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
        send(&mut framed, ClientMessage::Auth { token }).await?;
        let Some(frame) = framed.next().await.transpose()? else {
            return Err(anyhow!("connection closed during auth"));
        };
        let msg: ServerMessage = bincode::deserialize(&frame)?;
        match msg {
            ServerMessage::Authed => Ok(Self { framed }),
            ServerMessage::Error(e) => Err(anyhow!("auth failed: {e}")),
            other => Err(anyhow!("unexpected auth response: {:?}", other)),
        }
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
}

async fn send(framed: &mut Framed<TcpStream, LengthDelimitedCodec>, msg: ClientMessage) -> Result<()> {
    let data = bincode::serialize(&msg)?;
    framed.send(Bytes::from(data)).await?;
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
