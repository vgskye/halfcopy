use std::{path::{Path, PathBuf}, time::Duration};

use console::style;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use iroh::Endpoint;
use iroh_blobs::{
    api::{
        blobs::{ExportMode, ExportOptions},
        remote::GetProgressItem,
    },
    format::collection::Collection,
    get::{Stats, request::get_hash_seq_and_sizes},
    store::fs::FsStore,
    ticket::BlobTicket,
};
use tracing::trace;

use crate::coupon::{CouponMachineConfig, receive_coupon};


fn untimed_pb(step: &str, desc: &'static str) -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.set_style(
        ProgressStyle::with_template(
            "{prefix}{spinner:.green} {msg} [{elapsed_precise}]",
        )
        .unwrap(),
    );
    pb.set_prefix(format!("{} ", style(step).bold().dim()));
    pb.set_message(desc);
    pb.enable_steady_tick(Duration::from_millis(250));
    pb
}

fn make_download_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    pb.set_style(
        ProgressStyle::with_template("{prefix}{spinner:.green}{msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_prefix(format!("{} ", style("[4/5]").bold().dim()));
    pb.set_message("Downloading ...".to_string());
    pb
}

fn make_export_overall_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    pb.set_style(
        ProgressStyle::with_template("{prefix}{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {human_pos}/{human_len} {per_sec}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_prefix(format!("{}", style("[5/5]").bold().dim()));
    pb
}

pub async fn recv(coupon: &str) -> anyhow::Result<()> {
    let endpoint = Endpoint::builder()
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .bind()
        .await?;
    
    let mp: MultiProgress = MultiProgress::new();

    let pb = mp.add(untimed_pb("[1/5]", "Retrieving ticket..."));
    let ticket: BlobTicket = receive_coupon(
        &CouponMachineConfig {
            url: "wss://couponmachine.skye.vg".to_owned(),
            realm: "halfcopy".to_owned(),
            password_length: 3,
        },
        coupon,
        endpoint.node_id(),
    )
    .await?;
    pb.finish_and_clear();
    let addr = ticket.node_addr().clone();

    let dir_name = format!(".halfcopy-recv-{}", ticket.hash().to_hex());
    let iroh_data_dir = std::env::current_dir()?.join(dir_name);
    let db = FsStore::load(&iroh_data_dir).await?;

    let hash_and_format = ticket.hash_and_format();
    let local = db.remote().local(hash_and_format).await?;
    let (stats, total_files, payload_size) = if !local.is_complete() {
        let pb = mp.add(untimed_pb("[2/5]", "Connecting..."));
        let connection = endpoint.connect(addr, iroh_blobs::protocol::ALPN).await?;
        pb.finish_and_clear();
        let pb = mp.add(untimed_pb("[3/5]", "Getting sizes..."));
        let (_hash_seq, sizes) =
            get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                .await?;
        pb.finish_and_clear();
        let total_size = sizes.iter().copied().sum::<u64>();
        let payload_size = sizes.iter().skip(2).copied().sum::<u64>();
        let total_files = (sizes.len().saturating_sub(1)) as u64;
        eprintln!(
            "getting collection {} {} files, {}",
            ticket.hash().to_hex(),
            total_files,
            payload_size
        );
        let mut position = local.local_bytes();
        let op = mp.add(make_download_progress());
        op.set_length(total_size);
        op.set_position(position);
        let get = db.remote().execute_get(connection, local.missing());
        let mut stats = Stats::default();
        let mut stream = get.stream();
        while let Some(item) = stream.next().await {
            trace!("got item {item:?}");
            match item {
                GetProgressItem::Progress(offset) => {
                    position += offset;
                    op.set_position(position);
                }
                GetProgressItem::Done(value) => {
                    stats = value;
                    break;
                }
                GetProgressItem::Error(cause) => {
                    anyhow::bail!(cause);
                }
            }
        }
        op.finish_and_clear();
        (stats, total_files, payload_size)
    } else {
        println!("{} already complete", hash_and_format.hash);
        let total_files = local.children().unwrap() - 1;
        let payload_bytes = 0; // todo local.sizes().skip(2).map(Option::unwrap).sum::<u64>();
        (Stats::default(), total_files, payload_bytes)
    };

    let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;
    let root = std::env::current_dir()?;

    let op = mp.add(make_export_overall_progress());
    op.set_length(collection.len() as u64);
    for (i, (name, hash)) in collection.iter().enumerate() {
        op.set_position(i as u64);
        let target = get_export_path(&root, name)?;
        if target.exists() {
            eprintln!(
                "target {} already exists. Export stopped.",
                target.display()
            );
            eprintln!(
                "You can remove the file or directory and try again. The download will not be repeated."
            );
            anyhow::bail!("target {} already exists", target.display());
        }
        db.export_with_opts(ExportOptions {
            hash: *hash,
            target,
            mode: ExportMode::Copy,
        })
        .await?;
    }
    op.finish_and_clear();
    tokio::fs::remove_dir_all(iroh_data_dir).await?;
    Ok(())
}

fn get_export_path(root: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let parts = name.split('/');
    let mut path = root.to_path_buf();
    for part in parts {
        validate_path_component(part)?;
        path.push(part);
    }
    Ok(path)
}

fn validate_path_component(component: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !component.contains('/'),
        "path components must not contain the only correct path separator, /"
    );
    Ok(())
}
