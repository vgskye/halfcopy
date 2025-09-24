use std::{
    collections::BTreeMap, path::{Component, Path}, sync::Arc, time::Duration
};

use anyhow::{Context, anyhow};
use async_walkdir::WalkDir;
use dashmap::{DashMap, DashSet};
use futures_util::{stream::FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use iroh::{Endpoint, NodeId, Watcher, protocol::AccessLimit};
use iroh_blobs::{
    api::blobs::{AddPathOptions, AddProgressItem, ImportMode}, format::collection::Collection, provider::{self, events::{ConnectMode, EventMask, EventSender, ProviderMessage, RequestUpdate}}, store::fs::FsStore, ticket::BlobTicket, BlobFormat, BlobsProtocol
};
use itertools::Itertools;
use rand::Rng;
use tokio::{fs::canonicalize, sync::mpsc, task::JoinHandle};
use tracing::{error, trace};

use crate::coupon::{CouponMachineConfig, provide_coupon};

fn make_import_overall_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb
}

struct AbortOnDropHandle<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug)]
struct PerConnectionProgress {
    node_id: String,
    requests: BTreeMap<u64, ProgressBar>,
}

async fn per_request_progress(
    mp: MultiProgress,
    connection_id: u64,
    request_id: u64,
    connections: Arc<DashMap<u64, PerConnectionProgress>>,
    mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
) {
    let pb = mp.add(ProgressBar::hidden());
    let node_id = if let Some(mut connection) = connections.get_mut(&connection_id) {
        connection.requests.insert(request_id, pb.clone());
        connection.node_id.clone()
    } else {
        error!("got request for unknown connection {connection_id}");
        return;
    };
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
        ).unwrap()
        .progress_chars("#>-"),
    );
    while let Ok(Some(msg)) = rx.recv().await {
        match msg {
            RequestUpdate::Started(msg) => {
                pb.set_message(format!(
                    "n {} r {}/{} i {} # {}",
                    node_id,
                    connection_id,
                    request_id,
                    msg.index,
                    msg.hash.fmt_short()
                ));
                pb.set_length(msg.size);
            }
            RequestUpdate::Progress(msg) => {
                pb.set_position(msg.end_offset);
            }
            RequestUpdate::Completed(_) => {
                if let Some(mut msg) = connections.get_mut(&connection_id) {
                    msg.requests.remove(&request_id);
                };
            }
            RequestUpdate::Aborted(_) => {
                if let Some(mut msg) = connections.get_mut(&connection_id) {
                    msg.requests.remove(&request_id);
                };
            }
        }
    }
    pb.finish_and_clear();
    mp.remove(&pb);
}

async fn show_provide_progress(
    mp: MultiProgress,
    mut recv: mpsc::Receiver<ProviderMessage>,
) -> anyhow::Result<()> {
    let connections = Arc::new(DashMap::new());
    let mut tasks = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            item = recv.recv() => {
                let Some(item) = item else {
                    break;
                };

                trace!("got event {item:?}");
                match item {
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        let node_id = msg.node_id.map(|id| id.fmt_short()).unwrap_or_else(|| "?".to_string());
                        let connection_id = msg.connection_id;
                        connections.insert(
                            connection_id,
                            PerConnectionProgress {
                                requests: BTreeMap::new(),
                                node_id,
                            },
                        );
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        if let Some((_, connection)) = connections.remove(&msg.connection_id) {
                            for pb in connection.requests.values() {
                                pb.finish_and_clear();
                                mp.remove(pb);
                            }
                        }
                    }
                    ProviderMessage::GetRequestReceivedNotify(msg) => {
                        let request_id = msg.request_id;
                        let connection_id = msg.connection_id;
                        let connections = connections.clone();
                        let mp = mp.clone();
                        tasks.push(per_request_progress(mp, connection_id, request_id, connections, msg.rx));
                    }
                    _ => {}
                }
            }
            Some(_) = tasks.next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.next().await.is_some() {}
    Ok(())
}

pub async fn send(path: &Path) -> anyhow::Result<()> {
    let endpoint = Endpoint::builder()
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .bind()
        .await?;

    let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
    let cwd = canonicalize(std::env::current_dir()?).await?;
    let mut blobs_data_dir = cwd.join(format!(".halfcopy-send-{}", hex::encode(suffix)));
    while blobs_data_dir.exists() {
        let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
        blobs_data_dir = cwd.join(format!(".halfcopy-send-{}", hex::encode(suffix)));
    }

    let canonicalized = canonicalize(path).await?;
    if canonicalized == cwd {
        return Err(anyhow!("can not share from the current directory"));
    }

    let mp = MultiProgress::new();
    let (progress_tx, progress_rx) = mpsc::channel(32);
    let progress = AbortOnDropHandle(tokio::spawn(show_provide_progress(
        mp.clone(),
        progress_rx,
    )));

    let store = FsStore::load(&blobs_data_dir).await?;
    let blobs = BlobsProtocol::new(
        &store,
        endpoint.clone(),
        Some(EventSender::new(
            progress_tx,
            EventMask {
                connected: ConnectMode::Notify,
                get: provider::events::RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        )),
    );

    let mut name_and_tags = vec![];

    let root = canonicalized
        .parent()
        .context("Shared path has no parent! Are you trying to share root?")?;
    if canonicalized.is_file() {
        let relative = canonicalized.strip_prefix(root)?;
        if relative.to_str().is_none() {
            anyhow::bail!("Path {} is invalid for sharing!", relative.display());
        }
        let name = relative
            .components()
            .filter_map(|component| {
                if let Component::Normal(component) = component {
                    component.to_str()
                } else {
                    None
                }
            })
            .join("/");
        let import = store.add_path_with_opts(AddPathOptions {
            path: canonicalized.clone(),
            mode: ImportMode::TryReference,
            format: BlobFormat::Raw,
        });
        let mut stream = import.stream().await;
        let mut item_size = 0;
        let temp_tag = loop {
            let item = stream
                .next()
                .await
                .context("import stream ended without a tag")?;
            trace!("importing {} {item:?}", relative.display());
            match item {
                AddProgressItem::Size(size) => {
                    item_size = size;
                }
                AddProgressItem::Error(cause) => {
                    anyhow::bail!("error importing {}: {}", relative.display(), cause);
                }
                AddProgressItem::Done(tt) => {
                    break tt;
                }
                _ => {}
            }
        };
        name_and_tags.push((name, temp_tag, item_size));
    } else {
        let mut data_sources = vec![];
        let mut entries = WalkDir::new(&canonicalized);
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if entry.file_type().await?.is_file() {
                data_sources.push(entry.path());
            }
        }
        let op = mp.add(make_import_overall_progress());
        op.set_message(format!("importing {} files", data_sources.len()));
        op.set_length(data_sources.len() as u64);
        for (i, path) in data_sources.into_iter().enumerate() {
            op.set_position(i as u64);
            let relative = path.strip_prefix(root)?;
            if relative.to_str().is_none() {
                anyhow::bail!("Path {} is invalid for sharing!", relative.display());
            }
            let name = relative
                .components()
                .filter_map(|component| {
                    if let Component::Normal(component) = component {
                        component.to_str()
                    } else {
                        None
                    }
                })
                .join("/");
            let import = store.add_path_with_opts(AddPathOptions {
                path: path.clone(),
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            });
            let mut stream = import.stream().await;
            let mut item_size = 0;
            let temp_tag = loop {
                let item = stream
                    .next()
                    .await
                    .context("import stream ended without a tag")?;
                trace!("importing {} {item:?}", relative.display());
                match item {
                    AddProgressItem::Size(size) => {
                        item_size = size;
                    }
                    AddProgressItem::Error(cause) => {
                        anyhow::bail!("error importing {}: {}", relative.display(), cause);
                    }
                    AddProgressItem::Done(tt) => {
                        break tt;
                    }
                    _ => {}
                }
            };
            name_and_tags.push((name, temp_tag, item_size));
        }
        op.finish_and_clear();
    }
    name_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));

    let (collection, tags) = name_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, *tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(&store).await?;
    drop(tags);

    let allowed_visitors: Arc<DashSet<NodeId>> = Arc::new(DashSet::new());
    let allowed_visitors_2 = allowed_visitors.clone();

    let limited = AccessLimit::new(blobs, move |id| allowed_visitors_2.contains(&id));

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, limited)
        .spawn();

    let remote = provide_coupon(
        &CouponMachineConfig {
            url: "wss://couponmachine.skye.vg".to_owned(),
            realm: "halfcopy".to_owned(),
            password_length: 3,
        },
        |coupon| async move {
            println!("{coupon}");
            Ok(())
        },
        async {
            let addr = router.endpoint().node_addr().initialized().await;
            let ticket = BlobTicket::new(addr, *temp_tag.hash(), BlobFormat::HashSeq);
            Ok(ticket)
        },
    )
    .await?;
    allowed_visitors.insert(remote);

    tokio::signal::ctrl_c().await?;

    drop(progress);
    drop(temp_tag);

    println!("shutting down");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    tokio::fs::remove_dir_all(blobs_data_dir).await?;
    drop(router);
    Ok(())
}
