use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum ListKind {
    Worlds,
    Schematics,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum EntryKind {
    World,
    SchematicFile,
    SchematicDir,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RemoteEntry {
    pub root: String,
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DownloadMode {
    WorldZip,
    RegionZip,
    SchematicSingle,
    SchematicBundle,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMessage {
    Auth { token: String },
    List { kind: ListKind },
    RequestDownload {
        entries: Vec<RemoteEntry>,
        mode: DownloadMode,
    },
    RequestDownloadParallel {
        entries: Vec<RemoteEntry>,
        mode: DownloadMode,
        streams: u32,
    },
    RangeRequest {
        transfer_id: String,
        offset: u64,
        len: u64,
    },
    ReleaseTransfer {
        transfer_id: String,
    },
    Ping,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ProgressPhase {
    Scanning,
    Zipping,
    Sending,
    Completed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProgressUpdate {
    pub phase: ProgressPhase,
    pub done: u64,
    pub total: u64,
    pub label: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ServerMessage {
    Authed,
    Error(String),
    ListResponse { entries: Vec<RemoteEntry> },
    Progress(ProgressUpdate),
    DataChunk(Bytes),
    Stats {
        zip_ms: u128,
        send_wall_ms: u128,
        bytes: u64,
        avg_mb_s: f64,
        read_ms: u128,
        send_ms: u128,
        max_read_ms: u128,
        max_send_ms: u128,
        chunks: u64,
    },
    TransferReady {
        transfer_id: String,
        size: u64,
        label: String,
    },
    RangeChunk {
        offset: u64,
        data: Bytes,
    },
    Done,
}

pub const FRAME_LIMIT: usize = 32 * 1024 * 1024;
pub const CHUNK: usize = 8 * 1024 * 1024;
