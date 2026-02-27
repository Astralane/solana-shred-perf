mod leader_schedule_cache;

use crate::leader_schedule_cache::fetch_leader_schedule_cache;
use clap::Parser;
use futures_util::future::join_all;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use solana_ledger::shred::{Shred, ShredId, ShredType};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time;

#[derive(Debug, Clone, Deserialize, Hash)]
pub struct Provider {
    pub name: String,
    pub port: u16,
}

#[derive(Deserialize, Debug)]
struct Config {
    pub providers: Vec<Provider>,
    pub rpc_url: String,
    pub timeout_secs: u64,
    pub csv_file: Option<String>,
}
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    config: String,
}

#[derive(Debug)]
enum ProcessorEvent {
    ShredReceived {
        slot: u64,
        provider: Arc<Provider>,
        shred_id: ShredId,
        timestamp: SystemTime,
        data: Shred,
    },
    Cleanup,
    StatsTick,
}

#[derive(Default)]
struct ProcessorState {
    data: HashMap<u16, HashMap<u64, HashMap<ShredId, SystemTime>>>,
    highest_slot: Arc<AtomicU64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();
    let args = Args::parse();
    let config_file: String = read_to_string(args.config)?;

    let config: Config = serde_json::from_str(&config_file)?;
    let rpc = RpcClient::new(config.rpc_url);
    let leader_schedule_cache = fetch_leader_schedule_cache(&rpc).await?;

    let (processor_tx, mut processor_rx) = mpsc::channel(1024 * 100);

    let primary_provider = config.providers[0].clone();
    let mut tasks = Vec::with_capacity(config.providers.len());
    for provider in config.providers {
        let provider_c = Arc::new(provider);
        let jh = tokio::spawn(start_port_listener(provider_c, processor_tx.clone()));
        tasks.push(jh)

    }
    let listener_tasks = join_all(tasks);
    let timer_task = {
        let processor_tx = processor_tx.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = time::interval(Duration::from_secs(config.timeout_secs));
            let mut stats_interval = time::interval(Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        processor_tx.send(ProcessorEvent::Cleanup).await.ok();
                    }
                    _ = stats_interval.tick() => {
                        processor_tx.send(ProcessorEvent::StatsTick).await.ok();
                    }
                }
            }
        })
    };

    let processor_task = tokio::spawn(async move {
        let mut state = ProcessorState {
            ..Default::default()
        };

        let mut dedup: HashSet<(u16, ShredId)> = HashSet::new();
        while let Some(event) = processor_rx.recv().await {
            match event {
                ProcessorEvent::ShredReceived {
                    slot,
                    provider,
                    shred_id,
                    timestamp,
                    data,
                } => {
                    let leader = leader_schedule_cache
                        .get(&slot)
                        .expect("slot not in schedule");

                    if !data.verify(leader) {
                        warn!(
                            "cannot verify shreds given by provider {:?} for {slot} {leader:?}",
                            provider.name
                        )
                    }
                    if dedup.contains(&(provider.port, shred_id)) {
                        continue;
                    }
                    dedup.insert((provider.port, shred_id));
                    if slot > state.highest_slot.load(Ordering::Relaxed) {
                        state.highest_slot.store(slot, Ordering::Relaxed);
                    }
                    if slot
                        < state
                            .highest_slot
                            .load(Ordering::Relaxed)
                            .saturating_sub(10)
                    {
                        warn!(
                            "skipping, provider sent data 10 slots behind, provider {} highest slot {}, slot recvd {}",
                            provider.name, state.highest_slot.load(Ordering::Relaxed), slot
                        );
                        continue;
                    }
                    process_shred(&mut state, &provider, data, timestamp);
                }
                ProcessorEvent::Cleanup => {
                    // wtr.flush().unwrap();
                    // cleanup_data(&mut state, Duration::from_secs(args.timeout_secs));
                }
                ProcessorEvent::StatsTick => {
                    print_avg_time_diff(primary_provider.port, &state.data);
                }
            }
        }
    });

    tokio::select! {
        _ = listener_tasks => {},
        _ = processor_task => {},
        _ = timer_task => {},
        _ = tokio::signal::ctrl_c() => info!("Shutting down..."),
    }

    Ok(())
}

fn start_port_listener(
    provider: Arc<Provider>,
    sender: mpsc::Sender<ProcessorEvent>,
) -> tokio::task::JoinHandle<()> {
    let port = provider.port;
    tokio::spawn(async move {
        let socket = match UdpSocket::bind(format!("0.0.0.0:{}", port)).await {
            Ok(s) => s,
            Err(e) => {
                error!("[{}] Failed to bind port {}: {}", provider.name, port, e);
                return;
            }
        };
        info!("[{}] Listening on port {}", provider.name, port);

        let mut buf = [0u8; 2048];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, _)) => {
                    let data = buf[..size].to_vec();
                    if let Ok(shred) = Shred::new_from_serialized_shred(data) {
                        let slot = shred.slot() as u64;
                        let event = ProcessorEvent::ShredReceived {
                            slot,
                            provider: provider.clone(),
                            shred_id: shred.id(),
                            timestamp: SystemTime::now(),
                            data: shred,
                        };
                        if let Err(e) = sender.send(event).await {
                            error!("[{}] Failed to send event: {}", provider.name, e);
                        }
                    }
                }
                Err(e) => error!("[{}] Receive error: {}", provider.name, e),
            }
        }
    })
}

fn process_shred(
    state: &mut ProcessorState,
    provider: &Arc<Provider>,
    shred: Shred,
    timestamp: SystemTime,
) {
    state
        .data
        .entry(provider.port)
        .or_default()
        .entry(shred.slot())
        .or_default()
        .entry(shred.id())
        .or_insert(timestamp);
}

fn print_avg_time_diff(
    primary_port: u16,
    data: &HashMap<u16, HashMap<u64, HashMap<ShredId, SystemTime>>>,
) {
    let all_slots: std::collections::HashSet<u64> = data
        .values()
        .flat_map(|slots| slots.keys().copied())
        .collect();

    let primary_slots = match data.get(&primary_port) {
        Some(s) => s,
        None => {
            error!("primary port {} not found", primary_port);
            return;
        }
    };

    for slot in &all_slots {
        let primary_shreds = match primary_slots.get(slot) {
            Some(s) => s,
            None => continue,
        };
        let primairy_shred_keys = primary_shreds.keys().collect::<HashSet<_>>();

        for (port, slots) in data {
            if *port == primary_port {
                continue;
            }

            let Some(other_shreds) = slots.get(slot) else {
                continue;
            };

            let other_shred_keys = other_shreds.keys().collect::<HashSet<_>>();

            let only_primary: Vec<_> = primairy_shred_keys.difference(&other_shred_keys).collect();
            let only_other: Vec<_> = other_shred_keys.difference(&primairy_shred_keys).collect();

            let mut win_diffs: Vec<Duration> = Vec::new();
            let mut lose_diffs: Vec<Duration> = Vec::new();

            for (shred_id, primary_ts) in primary_shreds {
                let Some(other_ts) = other_shreds.get(shred_id) else {
                    continue;
                };

                if primary_ts <= other_ts {
                    // primary arrived first (wins)
                    if let Ok(diff) = other_ts.duration_since(*primary_ts) {
                        win_diffs.push(diff);
                    }
                } else {
                    // primary arrived later (loses)
                    if let Ok(diff) = primary_ts.duration_since(*other_ts) {
                        lose_diffs.push(diff);
                    }
                }
            }

            let avg_win = if !win_diffs.is_empty() {
                win_diffs.iter().sum::<Duration>() / win_diffs.len() as u32
            } else {
                Duration::ZERO
            };

            let avg_lose = if !lose_diffs.is_empty() {
                lose_diffs.iter().sum::<Duration>() / lose_diffs.len() as u32
            } else {
                Duration::ZERO
            };

            info!(
                "slot={} | port {} vs port {} | wins={} avg_win={:?} | losses={} avg_loss={:?} | only_primary={} | only_other={}",
                slot,
                primary_port,
                port,
                win_diffs.len(),
                avg_win,
                lose_diffs.len(),
                avg_lose,
                only_primary.len(),
                only_other.len(),
            );
        }
    }
}
