use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io;
use std::net::IpAddr;
use std::process;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio::time;
use tracing::{debug, error, info, info_span, warn};
use tracing_futures::Instrument;

use crate::consumer::ConsumerInner;
use crate::message::MessageExt;
use crate::namesrv::NameServer;
use crate::producer::ProducerInner;
use crate::protocol::{
    request::{
        ConsumerSendMsgBackRequestHeader, CreateTopicRequestHeader, PullMessageRequestHeader,
        UnregisterClientRequestHeader,
    },
    RemotingCommand, RequestCode, ResponseCode,
};
use crate::remoting::RemotingClient;
use crate::resolver::NsResolver;
use crate::route::{TopicRouteData, MASTER_ID};
use crate::utils::client_ip_addr;
use crate::Error;

mod model;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub security_token: Option<String>,
}

impl Credentials {
    pub fn new<S: Into<String>>(access_key: S, secret_key: S) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            security_token: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum PullStatus {
    Found = 0,
    NoNewMsg = 1,
    NoMsgMatched = 2,
    OffsetIllegal = 3,
    BrokerTimeout = 4,
}

#[derive(Debug, Clone)]
pub struct PullResult {
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub status: PullStatus,
    pub suggest_which_broker_id: i64,
    pub message_exts: Vec<MessageExt>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub(crate) group_name: String,
    name_server_addrs: Vec<String>,
    client_ip: String,
    instance_name: String,
    pub(crate) unit_mode: bool,
    unit_name: String,
    vip_channel_enabled: bool,
    retry_times: usize,
    pub(crate) credentials: Option<Credentials>,
    pub(crate) namespace: String,
}

impl ClientOptions {
    pub fn new(group: &str) -> Self {
        Self {
            group_name: group.to_string(),
            name_server_addrs: Vec::new(),
            client_ip: client_ip(),
            instance_name: "DEFAULT".to_string(),
            unit_mode: false,
            unit_name: String::new(),
            vip_channel_enabled: false,
            retry_times: 3,
            credentials: None,
            namespace: String::new(),
        }
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            group_name: "DEFAULT_CONSUMER".to_string(),
            name_server_addrs: Vec::new(),
            client_ip: client_ip(),
            instance_name: "DEFAULT".to_string(),
            unit_mode: false,
            unit_name: String::new(),
            vip_channel_enabled: false,
            retry_times: 3,
            credentials: None,
            namespace: String::new(),
        }
    }
}

fn client_ip() -> String {
    client_ip_addr()
        .map(|addr| match addr {
            IpAddr::V4(v4) => v4.to_string(),
            IpAddr::V6(v6) => format!("[{}]", v6),
        })
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

#[derive(Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum ClientState {
    Created = 0,
    StartFailed = 1,
    Running = 2,
    Shutdown = 3,
}

#[derive(Debug, Clone)]
pub struct Client<R: NsResolver + Clone> {
    options: ClientOptions,
    remote_client: RemotingClient,
    consumers: Arc<Mutex<HashMap<String, Arc<Mutex<ConsumerInner>>>>>,
    producers: Arc<Mutex<HashMap<String, Arc<Mutex<ProducerInner>>>>>,
    pub(crate) name_server: NameServer<R>,
    state: Arc<AtomicU8>,
    shutdown_tx: Arc<Mutex<Option<broadcast::Sender<()>>>>,
}

impl<R> Client<R>
    where
        R: NsResolver + Clone + Send + Sync + 'static,
{
    pub fn new(options: ClientOptions, name_server: NameServer<R>) -> Self {
        let credentials = options.credentials.clone();
        Self {
            options,
            remote_client: RemotingClient::new(credentials),
            consumers: Arc::new(Mutex::new(HashMap::new())),
            producers: Arc::new(Mutex::new(HashMap::new())),
            name_server,
            state: Arc::new(AtomicU8::new(ClientState::Created.into())),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Get Client ID
    pub fn id(&self) -> String {
        let mut client_id = self.options.client_ip.clone() + "@";
        if self.options.instance_name == "DEFAULT" {
            client_id.push_str(&process::id().to_string());
        } else {
            client_id.push_str(&self.options.instance_name);
        }
        if !self.options.unit_name.is_empty() {
            client_id.push_str(&self.options.unit_name);
        }
        client_id
    }

    pub fn start(&self) {
        match ClientState::try_from(self.state.load(Ordering::SeqCst)).unwrap() {
            ClientState::Created => {
                self.state.store(ClientState::StartFailed.into(), Ordering::SeqCst);
                let (shutdown_tx, mut shutdown_rx1) = broadcast::channel(1);
                let mut shutdown_rx2 = shutdown_tx.subscribe();
                let mut shutdown_rx3 = shutdown_tx.subscribe();
                self.shutdown_tx.lock().replace(shutdown_tx);

                // Schedule update name server address
                let name_server = self.name_server.clone();
                tokio::spawn(async move {
                    let mut interval = time::interval(time::Duration::from_secs(2 * 60));
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                match name_server.update_name_server_address().await {
                                    Ok(_) => info!("name server addresses update succeed"),
                                    Err(err) => error!("name server address update failed: {:?}", err),
                                };
                            }
                            _ = shutdown_rx1.recv() => {
                                info!("client shutdown, stop updating name server domain info");
                                break;
                            }
                        }
                    }
                }.instrument(info_span!("update_name_server_address")));

                // Schedule update route info
                let client = self.clone();
                tokio::spawn(
                    async move {
                        // time::delay_for(time::Duration::from_millis(10)).await;
                        let mut interval = time::interval(time::Duration::from_secs(30));
                        loop {
                            tokio::select! {
                                _ = interval.tick() => {
                                    let _ = client.update_topic_route_info().await;
                                }
                                _ = shutdown_rx2.recv() => {
                                    info!("client shutdown, stop updating topic route info");
                                    break;
                                }
                            }
                        }
                    }.instrument(info_span!("update_topic_route_info")),
                );

                // Schedule send heartbeat to all brokers
                let client = self.clone();
                tokio::spawn(
                    async move {
                        // time::delay_for(time::Duration::from_secs(1)).await;
                        let mut interval = time::interval(time::Duration::from_secs(30));
                        loop {
                            tokio::select! {
                                _ = interval.tick() => {
                                    let _ = client.send_heartbeat_to_all_brokers().await;
                                }
                                _ = shutdown_rx3.recv() => {
                                    info!("client shutdown, stop sending heartbeat to all brokers");
                                    break;
                                }
                            }
                        }
                    }.instrument(info_span!("send_heartbeat_to_all_brokers")),
                );

                // Persist offset

                // Rebalance
                self.state.store(ClientState::Running.into(), Ordering::SeqCst);
            }
            _ => {}
        }
    }

    pub fn shutdown(&self) {
        match ClientState::try_from(
            self.state
                .swap(ClientState::Shutdown.into(), Ordering::Relaxed),
        )
            .unwrap()
        {
            ClientState::Shutdown => {} // shutdown already
            _ => {
                if let Some(tx) = &*self.shutdown_tx.lock() {
                    tx.send(()).unwrap();
                }
                self.remote_client.shutdown();
            }
        }
    }

    pub fn state(&self) -> ClientState {
        ClientState::try_from(self.state.load(Ordering::Relaxed))
            .unwrap_or(ClientState::StartFailed)
    }

    #[inline]
    pub async fn invoke(&self, addr: &str, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {
        Ok(self.remote_client.invoke(addr, cmd).await?)
    }

    #[inline]
    pub async fn invoke_timeout(
        &self,
        addr: &str,
        cmd: RemotingCommand,
        timeout: time::Duration,
    ) -> Result<RemotingCommand, Error> {
        time::timeout(timeout, self.remote_client.invoke(addr, cmd))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::TimedOut, e))?
    }

    #[inline]
    pub async fn invoke_oneway(&self, addr: &str, cmd: RemotingCommand) -> Result<(), Error> {
        Ok(self.remote_client.invoke_oneway(addr, cmd).await?)
    }

    pub async fn pull_message(
        &self,
        addr: &str,
        request: PullMessageRequestHeader,
    ) -> Result<PullResult, Error> {
        let cmd = RemotingCommand::with_header(RequestCode::PullMessage, request, Vec::new());
        let res = self.remote_client.invoke(addr, cmd).await?;
        let status = match ResponseCode::from_code(res.code())? {
            ResponseCode::Success => PullStatus::Found,
            ResponseCode::PullNotFound => PullStatus::NoNewMsg,
            ResponseCode::PullRetryImmediately => PullStatus::NoMsgMatched,
            ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
            _ => {
                return Err(Error::ResponseError {
                    code: res.code(),
                    message: format!(
                        "unknown response code: {}, remark: {}",
                        res.code(),
                        res.header.remark
                    ),
                });
            }
        };
        let ext_fields = &res.header.ext_fields;
        let max_offset = ext_fields
            .get("maxOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let min_offset = ext_fields
            .get("minOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let next_begin_offset = ext_fields
            .get("nextBeginOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let suggest_which_broker_id = ext_fields
            .get("suggestWhichBrokerId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        Ok(PullResult {
            next_begin_offset,
            min_offset,
            max_offset,
            suggest_which_broker_id,
            status,
            message_exts: Vec::new(),
            body: res.body,
        })
    }

    pub(crate) fn register_consumer(&self, group: &str, consumer: Arc<Mutex<ConsumerInner>>) {
        let mut consumers = self.consumers.lock();
        consumers.entry(group.to_string()).or_insert(consumer);
    }

    pub(crate) fn unregister_consumer(&self, group: &str) {
        let mut consumers = self.consumers.lock();
        consumers.remove(group);
    }

    pub(crate) fn register_producer(&self, group: &str, producer: Arc<Mutex<ProducerInner>>) {
        let mut producers = self.producers.lock();
        producers.entry(group.to_string()).or_insert(producer);
    }

    pub(crate) fn unregister_producer(&self, group: &str) {
        let mut producers = self.producers.lock();
        producers.remove(group);
    }

    fn rebalance_immediately(&self) {
        let consumers = self.consumers.lock();
        for consumer in consumers.values() {
            consumer.lock().rebalance();
        }
    }

    async fn send_heartbeat_to_all_brokers(&self) {
        use model::{HeartbeatData, ProducerData};

        let producer_data_set: Vec<ProducerData> = self
            .producers
            .lock()
            .keys()
            .map(|group_name| ProducerData {
                group_name: group_name.clone(),
            })
            .collect();
        // FIXME
        let consumer_data_set = Vec::new();
        if producer_data_set.is_empty() && consumer_data_set.is_empty() {
            debug!("sending heartbeat, but no producer and no consumer found");
            return;
        }
        let broker_address_map = self.name_server.broker_address_map();
        if broker_address_map.is_empty() {
            debug!("sending heartbeat, but no brokers found");
            return;
        }
        let heartbeat_data = HeartbeatData {
            client_id: self.id(),
            producer_data_set,
            consumer_data_set,
        };
        let hb_bytes = serde_json::to_vec(&heartbeat_data).unwrap();
        for (broker_name, broker_data) in broker_address_map {
            for (id, addr) in &broker_data.broker_addrs {
                if heartbeat_data.consumer_data_set.is_empty() && *id != 0 {
                    continue;
                }
                debug!(
                    broker_name = %broker_name,
                    broker_id = id,
                    broker_addr = %addr,
                    "try to send heart beat to broker",
                );
                let cmd = RemotingCommand::new(
                    RequestCode::Heartbeat.into(),
                    0,
                    String::new(),
                    HashMap::new(),
                    hb_bytes.clone(),
                );
                match time::timeout(
                    time::Duration::from_secs(3),
                    self.remote_client.invoke(addr, cmd),
                )
                    .await
                {
                    Ok(Ok(res)) => match ResponseCode::try_from(res.code()) {
                        Ok(ResponseCode::Success) => {
                            self.name_server.add_broker_version(
                                &broker_name,
                                addr,
                                res.header.version as i32,
                            );
                            info!(
                                broker_name = %broker_name,
                                broker_id = id,
                                broker_addr = %addr,
                                "send heart beat to broker success",
                            );
                        }
                        _ => {
                            warn!(
                                broker_name = %broker_name,
                                broker_id = id,
                                broker_addr = %addr,
                                code = res.code(),
                                "send heart beat to broker failed",
                            );
                        }
                    },
                    Ok(Err(err)) => warn!("send heart beat to broker {} error {:?}", id, err),
                    Err(_) => warn!("send heart beat to broker {} timed out", id),
                }
            }
        }
    }

    pub fn update_publish_info(&self, topic: &str, data: TopicRouteData, changed: bool) {
        debug!(
            route_data = ?data,
            changed = changed,
            "update publish info for topic {}",
            topic
        );
        let producers = self.producers.lock();
        for producer in producers.values() {
            let mut producer = producer.lock();
            let updated = if changed {
                true
            } else {
                producer.is_publish_topic_need_update(topic)
            };
            if updated {
                let mut publish_info = data.to_publish_info(topic);
                publish_info.have_topic_router_info = true;
                producer.update_topic_publish_info(topic, publish_info);
            }
        }
    }

    async fn update_topic_route_info(&self) {
        let mut topics = HashSet::new();
        {
            let producers = self.producers.lock();
            for producer in producers.values() {
                topics.extend(producer.lock().publish_topic_list());
            }
        }
        if topics.is_empty() {
            debug!("updating topic route info, but no topics found");
            return;
        }
        info!("update route info for topics: {:?}", topics);
        for topic in &topics {
            match self.name_server.update_topic_route_info(topic).await {
                Ok((route_data, changed)) => {
                    info!(route_data = ?route_data, changed = changed, "topic route info updated");
                    self.update_publish_info(topic, route_data, changed);
                }
                Err(err) => error!("update topic {} route info failed: {:?}", topic, err),
            }
        }
    }

    async fn unregister_client(&self, producer_group: &str, consumer_group: &str) {
        for broker_data in self.name_server.broker_address_map().values() {
            for broker_addr in broker_data.broker_addrs.values() {
                let header = UnregisterClientRequestHeader {
                    client_id: self.id(),
                    producer_group: producer_group.to_string(),
                    consumer_group: consumer_group.to_string(),
                };
                let cmd = RemotingCommand::with_header(RequestCode::UnregisterClient, header, Vec::new());
                match self.remote_client.invoke(broker_addr, cmd).await {
                    Ok(res) => {
                        if res.code() != ResponseCode::Success {
                            warn!(code = res.code(), remark = %res.header.remark, "unregister client failed");
                        }
                    }
                    Err(err) => warn!("unregister client failed: {:?}", err),
                }
            }
        }
    }

    pub async fn create_topic(&self, key: &str, new_topic: &model::TopicConfig) -> Result<(), Error> {
        let mut last_error = None;
        let mut create_ok_at_least_once = false;
        let route_data = self.name_server.query_topic_route_info(key).await?;
        let broker_datas = route_data.broker_datas;
        if broker_datas.is_empty() {
            return Err(Error::EmptyRouteData);
        }
        for broker_data in broker_datas {
            if let Some(addr) = broker_data.broker_addrs.get(&MASTER_ID) {
                for _ in 0..5usize {
                    let header = CreateTopicRequestHeader {
                        topic: new_topic.topic_name.clone(),
                        default_topic: "TBW102".to_string(), // FIXME
                        read_queue_nums: new_topic.read_queue_nums,
                        write_queue_nums: new_topic.write_queue_nums,
                        permission: new_topic.permission.bits(),
                        topic_filter_type: new_topic.topic_filter_type.to_string(),
                        topic_sys_flag: new_topic.topic_sys_flag,
                        order: new_topic.order,
                    };
                    let cmd = RemotingCommand::with_header(RequestCode::UpdateAndCreateTopic, header, Vec::new());
                    match self.remote_client.invoke(addr, cmd).await {
                        Ok(res) => {
                            if res.code() == ResponseCode::Success {
                                create_ok_at_least_once = true;
                                break;
                            }
                        }
                        Err(err) => last_error = Some(err),
                    }
                }
            }
        }
        if let Some(err) = last_error {
            if !create_ok_at_least_once {
                return Err(err);
            }
        }
        Ok(())
    }

    pub async fn send_message_back(
        &self,
        broker_addr: &str,
        msg: &MessageExt,
        delay_level: i32,
        max_reconsume_times: i32,
    ) -> Result<(), Error> {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: msg.commit_log_offset,
            group: self.options.group_name.clone(),
            delay_level,
            origin_msg_id: msg.msg_id.clone(),
            origin_topic: msg.message.topic.clone(),
            unit_mode: self.options.unit_mode,
            max_reconsume_times,
        };
        let cmd =
            RemotingCommand::with_header(RequestCode::ConsumerSendMsgBack, header, Vec::new());
        let res = self.remote_client.invoke(broker_addr, cmd).await?;
        if res.code() == ResponseCode::Success {
            Ok(())
        } else {
            Err(Error::ResponseError {
                code: res.code(),
                message: res.header.remark,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::{model::TopicConfig, Client, ClientOptions};
    use crate::namesrv::NameServer;
    use crate::resolver::{Resolver, StaticResolver};

    fn new_client() -> Client<Resolver> {
        let options = ClientOptions::default();
        let name_server = NameServer::new(
            Resolver::Static(StaticResolver::new(vec!["localhost:9876".to_string()])),
            options.credentials.clone(),
        ).unwrap();
        Client::new(options, name_server)
    }

    #[tokio::test]
    async fn test_client_create_topic() {
        let client = new_client();
        client
            .create_topic("DefaultCluster", &TopicConfig::new("test"))
            .await
            .unwrap();
    }
}
