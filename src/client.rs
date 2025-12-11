use crate::protocol::*;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::path::Path;
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
            match msg {
                ServerMessage::Progress(p) => {
                    let _ = progress_tx.send(p);
                }
                ServerMessage::DataChunk(chunk) => {
                    file.write_all(&chunk).await?;
                }
                ServerMessage::Stats { zip_ms, send_ms, bytes, avg_mb_s } => {
                    let _ = stats_tx.send(TransferStats {
                        zip_ms,
                        send_ms,
                        bytes,
                        avg_mb_s,
                    });
                }
                ServerMessage::Done => break,
                ServerMessage::Error(e) => return Err(anyhow!(e)),
                _ => {}
            }
        }

        Ok(())
    }
}

async fn send(framed: &mut Framed<TcpStream, LengthDelimitedCodec>, msg: ClientMessage) -> Result<()> {
    let data = bincode::serialize(&msg)?;
    framed.send(Bytes::from(data)).await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct TransferStats {
    pub zip_ms: u128,
    pub send_ms: u128,
    pub bytes: u64,
    pub avg_mb_s: f64,
}
