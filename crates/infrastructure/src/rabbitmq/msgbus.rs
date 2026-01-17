
// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! RabbitMQ/STOMP Message Bus Database Adapter
//! 
//! Provides message bus functionality using RabbitMQ with STOMP protocol
//! for both local development (Docker) and production (Amazon Active MQ).

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::stream::Stream;
use nautilus_common::{
    msgbus::{
        CLOSE_TOPIC,
        database::{BusMessage, DatabaseConfig, MessageBusConfig, MessageBusDatabaseAdapter},
    },
    runtime::get_runtime,
};
use nautilus_core::UUID4;
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::identifiers::TraderId;

use super::{await_handle, create_stomp_connection, get_stomp_destination, StompConnection, StompMessage};

const MSGBUS_PUBLISH: &str = "msgbus-publish";
const MSGBUS_STREAM: &str = "msgbus-stream";
const MSGBUS_HEARTBEAT: &str = "msgbus-heartbeat";
const HEARTBEAT_TOPIC: &str = "health:heartbeat";

/// RabbitMQ/STOMP Message Bus Database Adapter
/// 
/// Implements MessageBusDatabaseAdapter using RabbitMQ with STOMP protocol.
/// Supports both local RabbitMQ (Docker) and Amazon Active MQ (production).
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.infrastructure")
)]
pub struct RabbitMqMessageBusDatabase {
    /// The trader ID for this message bus database.
    pub trader_id: TraderId,
    /// The instance ID for this message bus database.
    pub instance_id: UUID4,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
    pub_handle: Option<tokio::task::JoinHandle<()>>,
    stream_rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    stream_handle: Option<tokio::task::JoinHandle<()>>,
    stream_signal: Arc<AtomicBool>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
    heartbeat_signal: Arc<AtomicBool>,
}

impl MessageBusDatabaseAdapter for RabbitMqMessageBusDatabase {
    type DatabaseType = RabbitMqMessageBusDatabase;

    /// Creates a new [`RabbitMqMessageBusDatabase`] instance.
    fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        let config_clone = config.clone();
        let db_config = config
            .database
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No database config"))?;

        let (pub_tx, pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();

        // Create publish task
        let pub_handle = Some(get_runtime().spawn(async move {
            if let Err(e) = publish_messages(pub_rx, trader_id, instance_id, config_clone).await {
                log::error!("Error in task '{MSGBUS_PUBLISH}': {e}");
            };
        }));

        // Conditionally create stream task and channel if external streams configured
        let external_streams = config.external_streams.clone().unwrap_or_default();
        let stream_signal = Arc::new(AtomicBool::new(false));
        let (stream_rx, stream_handle) = if !external_streams.is_empty() {
            let stream_signal_clone = stream_signal.clone();
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100_000);
            (
                Some(stream_rx),
                Some(get_runtime().spawn(async move {
                    if let Err(e) =
                        stream_messages(stream_tx, db_config, external_streams, stream_signal_clone)
                            .await
                    {
                        log::error!("Error in task '{MSGBUS_STREAM}': {e}");
                    }
                })),
            )
        } else {
            (None, None)
        };

        // Create heartbeat task
        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = if let Some(heartbeat_interval_secs) = config.heartbeat_interval_secs
        {
            let signal = heartbeat_signal.clone();
            let pub_tx_clone = pub_tx.clone();

            Some(get_runtime().spawn(async move {
                run_heartbeat(heartbeat_interval_secs, signal, pub_tx_clone).await
            }))
        } else {
            None
        };

        Ok(Self {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle,
            stream_rx,
            stream_handle,
            stream_signal,
            heartbeat_handle,
            heartbeat_signal,
        })
    }

    /// Returns whether the message bus database adapter publishing channel is closed.
    fn is_closed(&self) -> bool {
        self.pub_tx.is_closed()
    }

    /// Publishes a message with the given `topic` and `payload`.
    fn publish(&self, topic: String, payload: Bytes) {
        let msg = BusMessage { topic, payload };
        if let Err(e) = self.pub_tx.send(msg) {
            log::error!("Failed to send message: {e}");
        }
    }

    /// Closes the message bus database adapter.
    fn close(&mut self) {
        log::debug!("Closing RabbitMQ message bus");

        self.stream_signal.store(true, Ordering::Relaxed);
        self.heartbeat_signal.store(true, Ordering::Relaxed);

        if !self.pub_tx.is_closed() {
            let msg = BusMessage {
                topic: CLOSE_TOPIC.to_string(),
                payload: Bytes::new(), // Empty
            };
            if let Err(e) = self.pub_tx.send(msg) {
                log::error!("Failed to send close message: {e:?}");
            }
        }

        // Keep close sync for now to avoid async trait method
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                self.close_async().await;
            });
        });

        log::debug!("RabbitMQ message bus closed");
    }
}

impl RabbitMqMessageBusDatabase {
    /// Gets the stream receiver for this instance.
    pub fn get_stream_receiver(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.stream_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
    }

    /// Streams messages arriving on the stream receiver channel.
    pub fn stream(
        mut stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> impl Stream<Item = BusMessage> + 'static {
        async_stream::stream! {
            while let Some(msg) = stream_rx.recv().await {
                yield msg;
            }
        }
    }

    pub async fn close_async(&mut self) {
        await_handle(self.pub_handle.take(), MSGBUS_PUBLISH).await;
        await_handle(self.stream_handle.take(), MSGBUS_STREAM).await;
        await_handle(self.heartbeat_handle.take(), MSGBUS_HEARTBEAT).await;
    }
}

/// Publish messages to RabbitMQ via STOMP
pub async fn publish_messages(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    trader_id: TraderId,
    instance_id: UUID4,
    config: MessageBusConfig,
) -> anyhow::Result<()> {
    tracing::debug!("Starting RabbitMQ message publishing via STOMP");

    let db_config = config
        .database
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No database config"))?;
    
    let mut stomp_conn = create_stomp_connection(MSGBUS_PUBLISH, db_config.clone()).await?;
    let destination = get_stomp_destination(trader_id, instance_id, &config);

    // Buffering for batch operations
    let mut buffer: VecDeque<BusMessage> = VecDeque::new();
    let mut last_drain = Instant::now();
    let buffer_interval = Duration::from_millis(config.buffer_interval_ms.unwrap_or(0) as u64);

    loop {
        if last_drain.elapsed() >= buffer_interval && !buffer.is_empty() {
            drain_buffer(
                &mut stomp_conn,
                &destination,
                config.stream_per_topic,
                &mut buffer,
            )
            .await?;
            last_drain = Instant::now();
        } else {
            match rx.recv().await {
                Some(msg) => {
                    if msg.topic == CLOSE_TOPIC {
                        tracing::debug!("Received close message");
                        drop(rx);
                        break;
                    }
                    buffer.push_back(msg);
                }
                None => {
                    tracing::debug!("Channel hung up");
                    break;
                }
            }
        }
    }

    // Drain any remaining messages
    if !buffer.is_empty() {
        drain_buffer(
            &mut stomp_conn,
            &destination,
            config.stream_per_topic,
            &mut buffer,
        )
        .await?;
    }

    tracing::debug!("Stopped RabbitMQ message publishing");
    Ok(())
}

/// Drain message buffer to RabbitMQ via STOMP
async fn drain_buffer(
    stomp_conn: &mut StompConnection,
    base_destination: &str,
    stream_per_topic: bool,
    buffer: &mut VecDeque<BusMessage>,
) -> anyhow::Result<()> {
    for msg in buffer.drain(..) {
        let destination = match stream_per_topic {
            true => format!("{}.{}", base_destination, msg.topic),
            false => base_destination.to_string(),
        };

        // Create STOMP message with topic and payload
        let stomp_payload = create_stomp_message_payload(&msg.topic, &msg.payload);
        
        stomp_conn.send(&destination, &stomp_payload).await?;
    }

    Ok(())
}

/// Create STOMP message payload with topic and data
fn create_stomp_message_payload(topic: &str, payload: &Bytes) -> Vec<u8> {
    // Create a simple JSON payload that includes both topic and data
    let json_msg = serde_json::json!({
        "topic": topic,
        "payload": payload.to_vec(),
        "timestamp": chrono::Utc::now().to_rfc3339()
    });
    
    json_msg.to_string().into_bytes()
}

/// Stream messages from RabbitMQ via STOMP
pub async fn stream_messages(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: DatabaseConfig,
    stream_destinations: Vec<String>,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    tracing::info!("Starting RabbitMQ message streaming via STOMP");
    let mut stomp_conn = create_stomp_connection(MSGBUS_STREAM, config).await?;

    // Subscribe to all destinations
    for destination in &stream_destinations {
        stomp_conn.subscribe(destination).await?;
        tracing::debug!("Subscribed to destination: {}", destination);
    }

    // Main streaming loop
    loop {
        if stream_signal.load(Ordering::Relaxed) {
            tracing::debug!("Received streaming terminate signal");
            break;
        }

        // Try to receive message with timeout
        match tokio::time::timeout(Duration::from_millis(100), stomp_conn.receive()).await {
            Ok(Ok(Some(stomp_msg))) => {
                match decode_stomp_message(stomp_msg) {
                    Ok(bus_msg) => {
                        if let Err(e) = tx.send(bus_msg).await {
                            tracing::debug!("Channel closed: {e:?}");
                            break; // End streaming
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error decoding STOMP message: {e:?}");
                        continue;
                    }
                }
            }
            Ok(Ok(None)) => {
                // No message received, continue
                continue;
            }
            Ok(Err(e)) => {
                tracing::error!("Error receiving STOMP message: {e:?}");
                continue;
            }
            Err(_) => {
                // Timeout, continue
                continue;
            }
        }
    }

    tracing::debug!("Stopped RabbitMQ message streaming");
    Ok(())
}

/// Decode STOMP message to BusMessage
fn decode_stomp_message(stomp_msg: StompMessage) -> anyhow::Result<BusMessage> {
    // Parse JSON payload
    let json_value: serde_json::Value = serde_json::from_slice(&stomp_msg.body)?;
    
    let topic = json_value
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing topic in STOMP message"))?
        .to_string();
    
    let payload_vec = json_value
        .get("payload")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("Missing payload in STOMP message"))?;
    
    let payload_bytes: Vec<u8> = payload_vec
        .iter()
        .filter_map(|v| v.as_u64().map(|n| n as u8))
        .collect();
    
    Ok(BusMessage {
        topic,
        payload: Bytes::from(payload_bytes),
    })
}

/// Run heartbeat task for RabbitMQ
async fn run_heartbeat(
    heartbeat_interval_secs: u16,
    signal: Arc<AtomicBool>,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
) {
    tracing::debug!("Starting RabbitMQ heartbeat at {heartbeat_interval_secs} second intervals");

    let heartbeat_interval = Duration::from_secs(heartbeat_interval_secs as u64);
    let heartbeat_timer = tokio::time::interval(heartbeat_interval);

    let check_interval = Duration::from_millis(100);
    let check_timer = tokio::time::interval(check_interval);

    tokio::pin!(heartbeat_timer);
    tokio::pin!(check_timer);

    loop {
        if signal.load(Ordering::Relaxed) {
            tracing::debug!("Received heartbeat terminate signal");
            break;
        }

        tokio::select! {
            _ = heartbeat_timer.tick() => {
                let heartbeat = create_heartbeat_msg();
                if let Err(e) = pub_tx.send(heartbeat) {
                    // We expect an error if the channel is closed during shutdown
                    tracing::debug!("Error sending heartbeat: {e}");
                }
            },
            _ = check_timer.tick() => {}
        }
    }

    tracing::debug!("Stopped RabbitMQ heartbeat");
}

/// Create heartbeat message
fn create_heartbeat_msg() -> BusMessage {
    BusMessage {
        topic: HEARTBEAT_TOPIC.to_string(),
        payload: Bytes::from(chrono::Utc::now().to_rfc3339().into_bytes()),
    }
}

////////////////////////////////////////////////////////////////////////////////
// Configuration
////////////////////////////////////////////////////////////////////////////////

/// RabbitMQ/STOMP specific configuration
#[derive(Debug, Clone)]
pub struct RabbitMqConfig {
    // Connection settings
    pub host: String,
    pub port: u16,
    pub virtual_host: String,
    pub username: String,
    pub password: String,
    
    // SSL/TLS settings (for Amazon Active MQ)
    pub use_ssl: bool,
    pub ssl_cert_path: Option<String>,
    pub ssl_key_path: Option<String>,
    pub ssl_ca_path: Option<String>,
    
    // Connection settings
    pub connection_timeout_secs: u64,
    pub heartbeat_interval_secs: u16,
    
    // Environment-specific settings
    pub environment: Environment,
}

#[derive(Debug, Clone)]
pub enum Environment {
    Development, // Local Docker RabbitMQ
    Production,  // Amazon Active MQ
}

impl Default for RabbitMqConfig {
    fn default() -> Self {
        Self {
            // Local Docker RabbitMQ defaults
            host: "localhost".to_string(),
            port: 61613, // STOMP port
            virtual_host: "/".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            
            // No SSL for local development
            use_ssl: false,
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_ca_path: None,
            
            connection_timeout_secs: 30,
            heartbeat_interval_secs: 30,
            
            environment: Environment::Development,
        }
    }
}

impl RabbitMqConfig {
    /// Create configuration for local Docker RabbitMQ
    pub fn local_docker() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 61613,
            virtual_host: "/".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            use_ssl: false,
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_ca_path: None,
            connection_timeout_secs: 30,
            heartbeat_interval_secs: 30,
            environment: Environment::Development,
        }
    }
    
    /// Create configuration for Amazon Active MQ
    pub fn amazon_active_mq(
        broker_endpoint: &str,
        username: &str,
        password: &str,
    ) -> Self {
        // Parse endpoint to extract host and port
        let (host, port) = if broker_endpoint.contains(':') {
            let parts: Vec<&str> = broker_endpoint.split(':').collect();
            (
                parts[0].to_string(),
                parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(61614) // Active MQ STOMP+SSL port
            )
        } else {
            (broker_endpoint.to_string(), 61614)
        };
        
        Self {
            host,
            port,
            virtual_host: "/".to_string(),
            username: username.to_string(),
            password: password.to_string(),
            
            // SSL enabled for Amazon Active MQ
            use_ssl: true,
            ssl_cert_path: None, // Use system CA certificates
            ssl_key_path: None,
            ssl_ca_path: None,
            
            connection_timeout_secs: 30,
            heartbeat_interval_secs: 30,
            environment: Environment::Production,
        }
    }
    
    /// Create configuration from environment variables
    pub fn from_env() -> anyhow::Result<Self> {
        let environment = match std::env::var("RABBITMQ_ENV")
            .unwrap_or_else(|_| "development".to_string())
            .as_str()
        {
            "production" => Environment::Production,
            _ => Environment::Development,
        };
        
        match environment {
            Environment::Development => Ok(Self::local_docker()),
            Environment::Production => {
                let broker_endpoint = std::env::var("ACTIVEMQ_BROKER_ENDPOINT")
                    .map_err(|_| anyhow::anyhow!("ACTIVEMQ_BROKER_ENDPOINT environment variable required for production"))?;
                let username = std::env::var("ACTIVEMQ_USERNAME")
                    .map_err(|_| anyhow::anyhow!("ACTIVEMQ_USERNAME environment variable required for production"))?;
                let password = std::env::var("ACTIVEMQ_PASSWORD")
                    .map_err(|_| anyhow::anyhow!("ACTIVEMQ_PASSWORD environment variable required for production"))?;
                
                Ok(Self::amazon_active_mq(&broker_endpoint, &username, &password))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_create_stomp_message_payload() {
        let topic = "test_topic";
        let payload = Bytes::from("test_data");
        
        let stomp_payload = create_stomp_message_payload(topic, &payload);
        let json: serde_json::Value = serde_json::from_slice(&stomp_payload).unwrap();
        
        assert_eq!(json["topic"], "test_topic");
        assert_eq!(json["payload"], vec![116, 101, 115, 116, 95, 100, 97, 116, 97]); // "test_data" as bytes
        assert!(json["timestamp"].is_string());
    }

    #[rstest]
    fn test_decode_stomp_message() {
        let stomp_msg = StompMessage {
            destination: "/topic/test".to_string(),
            headers: HashMap::new(),
            body: r#"{"topic": "test_topic", "payload": [116, 101, 115, 116], "timestamp": "2024-01-01T00:00:00Z"}"#.into(),
        };
        
        let bus_msg = decode_stomp_message(stomp_msg).unwrap();
        assert_eq!(bus_msg.topic, "test_topic");
        assert_eq!(bus_msg.payload, Bytes::from("test"));
    }

    #[rstest]
    fn test_rabbitmq_config_local_docker() {
        let config = RabbitMqConfig::local_docker();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 61613);
        assert_eq!(config.username, "guest");
        assert_eq!(config.password, "guest");
        assert!(!config.use_ssl);
        assert!(matches!(config.environment, Environment::Development));
    }

    #[rstest]
    fn test_rabbitmq_config_amazon_active_mq() {
        let config = RabbitMqConfig::amazon_active_mq(
            "b-123456-abcd-efgh.mq.us-east-1.amazonaws.com:61614",
            "admin",
            "password123"
        );
        
        assert_eq!(config.host, "b-123456-abcd-efgh.mq.us-east-1.amazonaws.com");
        assert_eq!(config.port, 61614);
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "password123");
        assert!(config.use_ssl);
        assert!(matches!(config.environment, Environment::Production));
    }
    
    #[rstest]
    fn test_create_heartbeat_msg() {
        let msg = create_heartbeat_msg();
        assert_eq!(msg.topic, HEARTBEAT_TOPIC);
        assert!(!msg.payload.is_empty());
        
        // Verify timestamp format
        let timestamp_str = String::from_utf8(msg.payload.to_vec()).unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(&timestamp_str).is_ok());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::time::Duration;
    
    // These tests would require actual RabbitMQ/Active MQ connections
    // Commented out for now but structure provided for future testing
    
    /*
    #[tokio::test]
    async fn test_rabbitmq_message_bus_local() {
        let trader_id = TraderId::from("test-trader");
        let instance_id = UUID4::new();
        let mut config = MessageBusConfig::default();
        
        // Configure for local RabbitMQ
        config.database = Some(DatabaseConfig {
            host: Some("localhost".to_string()),
            port: Some(61613),
            username: Some("guest".to_string()),
            password: Some("guest".to_string()),
            ssl: Some(false),
        });
        
        let mut msgbus = RabbitMqMessageBusDatabase::new(trader_id, instance_id, config).unwrap();
        
        // Test publishing
        msgbus.publish("test.topic".to_string(), Bytes::from("test payload"));
        
        // Allow time for message to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        msgbus.close();
    }
    
    #[tokio::test]
    async fn test_amazon_active_mq_connection() {
        // This test requires Active MQ credentials and would be run in CI/CD
        let config = RabbitMqConfig::amazon_active_mq(
            "ssl://b-123-456.mq.us-east-1.amazonaws.com:61614",
            "test-user",
            "test-password"
        );
        
        // Test connection establishment
        let conn = create_stomp_connection("test", DatabaseConfig::default()).await;
        assert!(conn.is_ok());
    }
    */
}