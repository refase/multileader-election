use std::{
    env,
    net::Ipv4Addr,
    str::{self, FromStr, Utf8Error},
    time::Duration,
};

use etcd_client::{Client, GetOptions, LockOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    // Environment variables.
    let trace_level = match env::var("TRACE_LEVEL")
        .expect("Trace level undefined")
        .as_str()
    {
        "TRACE" | "Trace" | "trace" => Level::TRACE,
        "INFO" | "Info" | "info" => Level::INFO,
        "DEBUG" | "Debug" | "debug" => Level::DEBUG,
        "WARN" | "Warn" | "warn" => Level::WARN,
        "ERROR" | "Error" | "error" => Level::ERROR,
        _ => Level::TRACE,
    };

    // Set up tracing.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(trace_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // let rt = tokio::runtime::Runtime::new().unwrap();

    // Using out struct.
    // let mut server = match rt.block_on(ServerNode::new()) {
    let mut server = match ServerNode::new().await {
        Ok(server) => {
            info!("Server created");
            server
        }
        Err(e) => {
            error!("Error creating server: {}", e);
            return Err(e);
        }
    };

    // // server.get_lease().await?;
    // rt.block_on(server.get_lease())?;
    // info!("Server lease: {:#?}", server.lease_id);

    server.query_election_prefixes().await?;
    // rt.block_on(server.query_election_prefixes())?;
    info!("Election leaders: {:#?}", server.election_leaders);
    if let Some(leader_key) = server.leader.clone() {
        info!("This node is a leader for {}", leader_key);
    } else {
        info!("Not a leader");
    }

    // rt.block_on(server.get_leaders())?;
    server.get_leaders().await?;

    loop {
        if server.leader.is_none() {
            // rt.block_on(server.query_election_prefixes()).unwrap();
            server.query_election_prefixes().await?;
        }

        if server.leader.is_some() {
            // rt.block_on(server.keep_alive()).unwrap();
            server.keep_alive().await?;
        }
    }

    // let handle = tokio::spawn(async move {
    //     server.keep_alive().await.unwrap();
    // });

    // match tokio::join!(handle) {
    //     (Ok(_),) => info!("Keep alive joined"),
    //     (Err(e),) => error!("Keep alive join error: {}", e),
    // }

    // Ok(())
}

type Key = String;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
struct ServiceNode {
    node: String,
    address: Ipv4Addr,
}

struct ServerNode {
    inner: Client,
    lease_ttl: i64,
    sleep_time: u64,
    leader: Option<Key>,
    svc_node: ServiceNode,
    election_key_prefixes: [String; 2],
    leader_keys: [String; 2],
    lease_id: Option<i64>,
    election_key: Option<String>,
    election_leaders: Option<Vec<ServiceNode>>,
}

impl Drop for ServerNode {
    fn drop(&mut self) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        if self.election_key.is_some() {
            let key = self.election_key.as_ref().unwrap();
            rt.block_on(self.inner.unlock(key.clone())).unwrap();
            info!("Unlocked the lock");
        }
        // revoke a lease
        let _resp = rt
            .block_on(self.inner.lease_revoke(self.lease_id.unwrap()))
            .unwrap();
        info!("revoke a lease with id {:?}", self.lease_id.unwrap());
    }
}

impl ServerNode {
    async fn new() -> Result<Self, ServerError> {
        let node = env::var("NODE").expect("Node name undefined");
        let dbname = env::var("DBNAME").expect("Database name undefined");
        let address = Ipv4Addr::from_str(
            env::var("ADDRESS")
                .expect("Node address undefined")
                .as_str(),
        )
        .expect("Invalid IPv4 address");
        let etcd = env::var("ETCD").expect("etcd address undefined");
        let lease_ttl = env::var("LEASE_TTL")
            .expect("Lease TTL undefined")
            .parse::<i64>()
            .expect("Invalid lease ttl");
        let sleep_time = env::var("SLEEP_TIME")
            .expect("Sleep time undefined")
            .parse::<u64>()
            .expect("Invalid sleep time");

        if !(lease_ttl > sleep_time as i64) {
            error!(
                "Sleep time({}) larger than lease ttl({})",
                sleep_time, lease_ttl
            );
            return Err(ServerError::ServerCreationError(format!(
                "Sleep time({}) larger than lease ttl({})",
                sleep_time, lease_ttl
            )));
        }

        let group_size = env::var("GROUP_SIZE")
            .expect("Size of each group for the cluster is undefined")
            .parse::<u64>()
            .expect("Invalid number for group size");

        let inner = Client::connect([etcd], None).await?;
        let svc_node = ServiceNode {
            node: node.clone(),
            address,
        };

        let node_num = match node.clone().split("-").collect::<Vec<&str>>().last() {
            Some(n_str) => match n_str.parse::<u64>() {
                Ok(n) => n,
                Err(e) => {
                    return Err(ServerError::ServerCreationError(format!(
                        "Invalid node number. Possibly not deployed as stateful set. {}",
                        e
                    )))
                }
            },
            None => {
                return Err(ServerError::ServerCreationError(
                    "Invalid node number. Possibly not deployed as stateful set".to_string(),
                ))
            }
        };

        let group_id = (node_num / group_size) + 1;

        let election_key_prefixes = [
            format!("{}-{}-1-leader", dbname, group_id),
            format!("{}-{}-2-leader", dbname, group_id),
        ];

        let leader_keys = [
            format!("{}-{}-1", dbname, group_id),
            format!("{}-{}-2", dbname, group_id),
        ];

        Ok(Self {
            inner,
            lease_ttl,
            sleep_time,
            leader: None,
            svc_node,
            election_key_prefixes,
            leader_keys,
            lease_id: None,
            election_key: None,
            election_leaders: None,
        })
    }

    async fn get_lease(&mut self) -> Result<(), ServerError> {
        let lease = self.inner.lease_grant(self.lease_ttl, None).await?;
        info!("Lease received: {:#?}", lease);
        self.lease_id = Some(lease.id());
        Ok(())
    }

    async fn query_election_prefixes(&mut self) -> Result<(), ServerError> {
        let getoptions = GetOptions::new().with_prefix();
        let election_prefixes = self.election_key_prefixes.clone();
        loop {
            // We do not want to become both leaders of the group.
            if self.leader.is_some() {
                break;
            }
            for election_key in election_prefixes.clone() {
                // We do not want to become both leaders of the group.
                if self.leader.is_some() {
                    break;
                }
                let get_resp = self
                    .inner
                    .get(election_key.as_str(), Some(getoptions.clone()))
                    .await?;

                let kvs = get_resp.kvs();
                match kvs.len() {
                    0 => {
                        match self.campaign(election_key.clone()).await {
                            Ok(_) => {
                                info!("Campaign done");
                            }
                            Err(e) => {
                                info!("Campaign error: {}", e);
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(Duration::from_secs(self.sleep_time)).await;
        }
        Ok(())
    }

    async fn get_leaders(&mut self) -> Result<(), ServerError> {
        let mut election_leaders = Vec::new();
        let leader_keys = self.leader_keys.clone();
        for leader_key in leader_keys {
            let resp = self.inner.get(leader_key, None).await?;
            let kvs = resp.kvs();
            let svc_node;
            match kvs.len() {
                0 => {}
                _ => {
                    svc_node = match bincode::deserialize::<ServiceNode>(kvs[0].value()) {
                        Ok(svc_node) => svc_node,
                        Err(e) => return Err(ServerError::SerializationError(e)),
                    };
                    election_leaders.push(svc_node);
                }
            }
        }
        self.election_leaders = Some(election_leaders);
        Ok(())
    }

    async fn campaign(&mut self, election_key: String) -> Result<(), ServerError> {
        self.get_lease().await?;
        if let Some(lease_id) = self.lease_id {
            // Lock with lease.
            let lock_options = LockOptions::new().with_lease(lease_id);
            let resp = self
                .inner
                .lock(election_key.clone(), Some(lock_options))
                .await?;
            let key = match str::from_utf8(resp.key()) {
                Ok(key) => key,
                Err(e) => return Err(ServerError::StringifyError(e)),
            };
            info!("Locking with lease: {}", key);

            let value = match bincode::serialize(&self.svc_node) {
                Ok(val) => val,
                Err(e) => return Err(ServerError::SerializationError(e)),
            };

            let leader_key;
            if self.election_key_prefixes[0].as_str() == election_key.clone().as_str() {
                leader_key = self.leader_keys[0].clone();
            } else {
                leader_key = self.leader_keys[1].clone();
            }

            let put_resp = self.inner.put(leader_key.clone(), value, None).await?;
            info!("Put response: {:#?}", put_resp);
            self.leader = Some(leader_key.to_owned());
        }
        Ok(())
    }

    async fn keep_alive(&mut self) -> Result<(), ServerError> {
        loop {
            tokio::time::sleep(Duration::from_secs(self.sleep_time)).await;
            if let Some(lease_id) = self.lease_id {
                // keep alive
                let (mut keeper, mut stream) = self.inner.lease_keep_alive(lease_id).await?;
                info!("lease {:?} keep alive start", lease_id);
                keeper.keep_alive().await?;
                if let Some(resp) = stream.message().await? {
                    info!("lease {:?} keep alive, new ttl {:?}", resp.id(), resp.ttl());
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    SerializationError(#[from] Box<bincode::ErrorKind>),
    #[error(transparent)]
    StringifyError(#[from] Utf8Error),
    #[error(transparent)]
    EtcdError(#[from] etcd_client::Error),
    #[error("Error creating election client: {}", 0)]
    ServerCreationError(String),
}
