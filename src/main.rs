use std::path::PathBuf;

use clap::Parser;

mod coupon;
mod recv;
mod send;
mod words;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
enum Args {
    Send { path: PathBuf },
    Recv { coupon: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args {
        Args::Send { path } => send::send(&path).await?,
        Args::Recv { coupon } => recv::recv(&coupon).await?,
    }
    Ok(())
}
