mod leader_schedule_cache;

use crate::leader_schedule_cache::fetch_leader_schedule_cache;
use clap::Parser;
use csv::Writer;
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

    //create a file from args. csv_file name if exits or create a file with name report_mm_dd_hh_mm_ss format
    let csv_file_name = if let Some(ref name) = config.csv_file {
        name.clone()
    } else {
        let now = chrono::Local::now();
        format!("report_{}.csv", now.format("%m_%d_%H_%M_%S"))
    };
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&csv_file_name)?;
    let mut wtr = csv::Writer::from_writer(file);

    let (processor_tx, mut processor_rx) = mpsc::channel(4096);

    let mut tasks = Vec::with_capacity(config.providers.len());
    for provider in config.providers {
        let provider_c = Arc::new(provider);
        tasks.push(start_port_listener(provider_c, processor_tx.clone()))
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

        while let Some(event) = processor_rx.recv().await {
            let mut dedup: HashSet<(u16, ShredId)> = HashSet::new();
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
                    process_shred(&mut state, &mut wtr, &provider, data, timestamp);
                }
                ProcessorEvent::Cleanup => {
                    wtr.flush().unwrap();
                    // cleanup_data(&mut state, Duration::from_secs(args.timeout_secs));
                }
                ProcessorEvent::StatsTick => {
                    print_avg_time_diff(&state.data);
                    //
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

#[derive(Debug, Serialize)]
struct Record {
    name: String,
    port: u16,
    slot: u64,
    shred_index: u32,
    shred_type: ShredType,
    ts: u64,
}

fn process_shred<T: io::Write>(
    state: &mut ProcessorState,
    writer: &mut Writer<T>,
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
        .insert(shred.id(), timestamp);

    writer
        .serialize(Record {
            name: provider.name.clone(),
            port: provider.port,
            slot: shred.slot(),
            shred_index: shred.index(),
            shred_type: ShredType::Data,
            ts: timestamp.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
        })
        .unwrap();
}

fn get_payload(shred: &Shred) -> &[u8] {
    let Ok(offset) = shred.retransmitter_signature_offset() else {
        return shred.payload();
    };
    // Assert that the retransmitter's signature is at the very end of
    // the shred payload.
    shred
        .payload()
        .get(..offset)
        .unwrap_or_else(|| shred.payload())
}

fn print_avg_time_diff(data: &HashMap<u16, HashMap<u64, HashMap<ShredId, SystemTime>>>) {
    // slot -> provider -> list of timestamps
    let mut slot_provider_times: HashMap<u64, HashMap<u16, Vec<SystemTime>>> = HashMap::new();

    for (port, slots) in data {
        for (slot, shreds) in slots {
            for timestamp in shreds.values() {
                slot_provider_times
                    .entry(*slot)
                    .or_default()
                    .entry(*port)
                    .or_default()
                    .push(*timestamp);
            }
        }
    }

    for (slot, providers) in &slot_provider_times {
        // compute avg timestamp per provider
        let avg_times: Vec<(u16, Duration)> = providers
            .iter()
            .map(|(provider_port, timestamps)| {
                let avg = timestamps
                    .iter()
                    .map(|t| t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default())
                    .sum::<Duration>()
                    / timestamps.len() as u32;
                (*provider_port, avg)
            })
            .collect();

        // print diff between each pair of providers
        for i in 0..avg_times.len() {
            for j in (i + 1)..avg_times.len() {
                let (p1, t1) = &avg_times[i];
                let (p2, t2) = &avg_times[j];
                let diff = if t1 > t2 { *t1 - *t2 } else { *t2 - *t1 };
                info!(
                    "slot={} | port {} vs port {} | avg diff = {:?}",
                    slot, p1, p2, diff
                );
            }
        }
    }
}
