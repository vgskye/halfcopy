use std::{
    path::{Component, Path},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, anyhow};
use async_walkdir::WalkDir;
use dashmap::DashSet;
use futures_util::StreamExt;
use iroh::{Endpoint, NodeId, Watcher, protocol::AccessLimit};
use iroh_blobs::{
    BlobFormat, BlobsProtocol,
    api::blobs::{AddPathOptions, AddProgressItem, ImportMode},
    format::collection::Collection,
    store::fs::FsStore,
    ticket::BlobTicket,
};
use itertools::Itertools;
use rand::Rng;
use tokio::fs::canonicalize;
use tracing::trace;

use crate::coupon::{CouponMachineConfig, provide_coupon};

pub async fn send(path: &Path) -> anyhow::Result<()> {
    let endpoint = Endpoint::builder()
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .bind()
        .await?;

    let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
    let cwd = canonicalize(std::env::current_dir()?).await?;
    let mut blobs_data_dir = cwd.join(format!(".sendme-send-{}", hex::encode(suffix)));
    while blobs_data_dir.exists() {
        let suffix = rand::thread_rng().r#gen::<[u8; 16]>();
        blobs_data_dir = cwd.join(format!(".sendme-send-{}", hex::encode(suffix)));
    }

    let canonicalized = canonicalize(path).await?;
    if canonicalized == cwd {
        return Err(anyhow!("can not share from the current directory"));
    }

    let store = FsStore::load(&blobs_data_dir).await?;
    let blobs = BlobsProtocol::new(&store, endpoint.clone(), None);

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
        let mut entries = WalkDir::new(&canonicalized);
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if entry.file_type().await?.is_file() {
                let path = entry.path();
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
        }
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

    drop(temp_tag);

    println!("shutting down");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    tokio::fs::remove_dir_all(blobs_data_dir).await?;
    drop(router);
    Ok(())
}
