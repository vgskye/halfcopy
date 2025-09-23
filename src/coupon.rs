use std::iter::repeat_with;

use anyhow::anyhow;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::Aead};
use futures_util::{SinkExt, StreamExt};
use iroh::NodeId;
use iroh_base::ticket::Ticket;
use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use spake2::{Ed25519Group, Identity, Password, Spake2};
use tokio_tungstenite::tungstenite::Message;

use crate::words::WORDS;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Hello {
    realm: String,
    proto: u64,
    message: Vec<u8>,
}

pub const CURRENT_PROTO: u64 = 0;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FirstMessage {
    Create(CouponCreateRequest),
    Redeem(CouponRedeemStartRequest),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponCreateRequest {
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponCreateResponse {
    header: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemStartedNotice {
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemStartedResponse {
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemFinishedNotice {
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemStartRequest {
    header: String,
    pake_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemStartResponse {
    pake_message: Vec<u8>,
    data_message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CouponRedeemFinishRequest {
    data_message: Vec<u8>,
}

pub struct CouponMachineConfig {
    pub url: String,
    pub realm: String,
    pub password_length: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CompactTicket {
    kind: String,
    data: Vec<u8>,
}

pub async fn provide_coupon<T: Future<Output = anyhow::Result<()>>, U: Ticket>(
    config: &CouponMachineConfig,
    coupon_callback: impl FnOnce(String) -> T,
    ticket_callback: impl Future<Output = anyhow::Result<U>>,
) -> anyhow::Result<NodeId> {
    let mut rng = thread_rng();
    let password = repeat_with(|| *WORDS.choose(&mut rng).unwrap())
        .take(config.password_length)
        .join("-");
    let (spake, pake_message) = Spake2::<Ed25519Group>::start_a(
        &Password::new(&password),
        &Identity::new(config.realm.as_bytes()),
        &Identity::new(config.url.as_bytes()),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&config.url).await?;
    let inner = postcard::to_stdvec(&FirstMessage::Create(CouponCreateRequest { pake_message }))?;
    let hello = postcard::to_stdvec(&Hello {
        realm: config.realm.clone(),
        proto: CURRENT_PROTO,
        message: inner,
    })?;
    ws.send(Message::binary(hello)).await?;
    let msg = ws
        .next()
        .await
        .ok_or(anyhow!("Unexpected connection close"))??;
    if msg.is_text() {
        return Err(anyhow!(
            "Server returned error: {}",
            msg.into_text().unwrap()
        ));
    }
    let msg: CouponCreateResponse = postcard::from_bytes(&msg.into_data())?;
    coupon_callback(format!("{}-{password}", msg.header)).await?;
    let redeem_msg = ws
        .next()
        .await
        .ok_or(anyhow!("Unexpected connection close"))??;
    if redeem_msg.is_text() {
        return Err(anyhow!(
            "Server returned error: {}",
            redeem_msg.into_text().unwrap()
        ));
    }
    let redeem_msg: CouponRedeemStartedNotice = postcard::from_bytes(&redeem_msg.into_data())?;
    let secret = spake.finish(&redeem_msg.pake_message)?;
    let hkdf = hkdf::Hkdf::<blake3::Hasher, hkdf::hmac::SimpleHmac<_>>::new(None, &secret);
    let mut provider_key = [0u8; 32];
    hkdf.expand(b"provider key", &mut provider_key)
        .map_err(|e| anyhow!("{e:?}"))?;
    let provider_cipher = ChaCha20Poly1305::new(&provider_key.into());

    let ticket = ticket_callback.await?;
    let ticket = postcard::to_stdvec(&CompactTicket {
        kind: U::KIND.to_owned(),
        data: ticket.to_bytes(),
    })?;

    let payload = provider_cipher.encrypt(b"niko oneshot".into(), ticket.as_slice())?;

    let resp = postcard::to_stdvec(&CouponRedeemStartedResponse {
        data_message: payload,
    })?;
    ws.send(Message::binary(resp)).await?;

    let finished_msg = ws
        .next()
        .await
        .ok_or(anyhow!("Unexpected connection close"))??;
    if finished_msg.is_text() {
        return Err(anyhow!(
            "Server returned error: {}",
            finished_msg.into_text().unwrap()
        ));
    }
    let finished_msg: CouponRedeemFinishedNotice = postcard::from_bytes(&finished_msg.into_data())?;

    let mut requester_key = [0u8; 32];
    hkdf.expand(b"requester key", &mut requester_key)
        .map_err(|e| anyhow!("{e:?}"))?;
    let requester_cipher = ChaCha20Poly1305::new(&requester_key.into());

    let payload =
        requester_cipher.decrypt(b"niko oneshot".into(), finished_msg.data_message.as_slice())?;
    let node_id = postcard::from_bytes(&payload)?;
    Ok(node_id)
}

pub async fn receive_coupon<T: Ticket>(
    config: &CouponMachineConfig,
    password: &str,
    node_id: NodeId,
) -> anyhow::Result<T> {
    let (header, password) = password.split_once('-').ok_or(anyhow!("Invalid coupon!"))?;
    let (spake, pake_message) = Spake2::<Ed25519Group>::start_b(
        &Password::new(password),
        &Identity::new(config.realm.as_bytes()),
        &Identity::new(config.url.as_bytes()),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&config.url).await?;
    let inner = postcard::to_stdvec(&FirstMessage::Redeem(CouponRedeemStartRequest {
        header: header.to_string(),
        pake_message,
    }))?;
    let hello = postcard::to_stdvec(&Hello {
        realm: config.realm.clone(),
        proto: CURRENT_PROTO,
        message: inner,
    })?;
    ws.send(Message::binary(hello)).await?;
    let msg = ws
        .next()
        .await
        .ok_or(anyhow!("Unexpected connection close"))??;
    if msg.is_text() {
        return Err(anyhow!(
            "Server returned error: {}",
            msg.into_text().unwrap()
        ));
    }
    let msg: CouponRedeemStartResponse = postcard::from_bytes(&msg.into_data())?;
    let secret = spake.finish(&msg.pake_message)?;
    let hkdf = hkdf::Hkdf::<blake3::Hasher, hkdf::hmac::SimpleHmac<_>>::new(None, &secret);
    let mut provider_key = [0u8; 32];
    hkdf.expand(b"provider key", &mut provider_key)
        .map_err(|e| anyhow!("{e:?}"))?;
    let provider_cipher = ChaCha20Poly1305::new(&provider_key.into());

    let ticket = provider_cipher.decrypt(b"niko oneshot".into(), msg.data_message.as_slice())?;
    let ticket: CompactTicket = postcard::from_bytes(&ticket)?;
    if ticket.kind != T::KIND {
        return Err(anyhow!("Bad ticket kind"));
    }
    let ticket = T::from_bytes(&ticket.data)?;

    let mut requester_key = [0u8; 32];
    hkdf.expand(b"requester key", &mut requester_key)
        .map_err(|e| anyhow!("{e:?}"))?;
    let requester_cipher = ChaCha20Poly1305::new(&requester_key.into());

    let payload = requester_cipher.encrypt(
        b"niko oneshot".into(),
        postcard::to_stdvec(&node_id)?.as_slice(),
    )?;

    let resp = postcard::to_stdvec(&CouponRedeemFinishRequest {
        data_message: payload,
    })?;
    ws.send(Message::binary(resp)).await?;
    ws.close(None).await?;

    Ok(ticket)
}
