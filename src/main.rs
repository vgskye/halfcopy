use std::env::args;

use iroh::{NodeAddr, SecretKey};
use iroh_base::ticket::NodeTicket;
use rand::thread_rng;

use crate::coupon::CouponMachineConfig;

mod coupon;
mod recv;
mod send;
mod words;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let key = SecretKey::generate(&mut thread_rng());
    println!("{:?}", key.public());
    if let Some(coupon) = args().nth(1) {
        let ticket: NodeTicket = coupon::receive_coupon(
            &CouponMachineConfig {
                url: "ws://localhost:8080".to_owned(),
                realm: "test".to_owned(),
                password_length: 2,
            },
            &coupon,
            key.public(),
        )
        .await?;
        println!("{ticket:?}");
    } else {
        let nodeid = coupon::provide_coupon(
            &CouponMachineConfig {
                url: "ws://localhost:8080".to_owned(),
                realm: "test".to_owned(),
                password_length: 2,
            },
            |coupon| async move {
                println!("{coupon}");
                Ok(())
            },
            async { Ok(NodeTicket::new(NodeAddr::new(key.public()))) },
        )
        .await?;
        println!("{nodeid:?}");
    }
    Ok(())
}
