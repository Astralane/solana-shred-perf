use solana_pubkey::Pubkey;
use solana_rpc_client::api::request::Address;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

pub type LeaderScheduleCache = Arc<HashMap<u64, Address>>;

pub async fn fetch_leader_schedule_cache(rpc: &RpcClient) -> anyhow::Result<LeaderScheduleCache> {
    let epoch_info = rpc.get_epoch_info().await?;
    let epoch_start_slot = epoch_info
        .absolute_slot
        .saturating_sub(epoch_info.slot_index);
    let leader_info = rpc
        .get_leader_schedule(Some(epoch_info.absolute_slot))
        .await?;
    let leader_schedule = leader_info.unwrap();
    let mut leader_schedule_map = HashMap::new();
    for (key, offsets) in leader_schedule.iter() {
        let pubkey = Pubkey::from_str(key).expect("unknown pubkey");
        for offest in offsets {
            leader_schedule_map.insert(
                epoch_start_slot.saturating_add(*offest as u64),
                pubkey.clone(),
            );
        }
    }
    let cache = LeaderScheduleCache::new(leader_schedule_map);
    Ok(cache)
}
