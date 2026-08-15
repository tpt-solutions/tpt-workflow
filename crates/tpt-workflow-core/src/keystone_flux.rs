//! Real-time CDC subscription client for tpt-keystone-db's Flux service.
//!
//! Provides a typed streaming view of workflow change events, used for
//! replay-on-crash and live-status updates. The gRPC contract is generated
//! from `flux.proto` (package `tpt.workflow.v1`); align the message shapes
//! with the upstream keystone Flux service when wiring against a real
//! deployment.

#[cfg(feature = "flux")]
mod generated {
    use crate::error::{Error, Result};
    use prost_types::value::Kind;
    use prost_types::Struct;
    use tokio::sync::mpsc;
    use tpt_workflow_proto::tpt::workflow::v1::flux_client::FluxClient;
    use tpt_workflow_proto::tpt::workflow::v1::{
        FluxChangeEvent as ProtoEvent, FluxChangeOp, FluxChangeSubscription, FluxEntityKind,
    };
    use tracing::warn;

    /// Kind of change observed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CdcOp {
        Insert,
        Update,
        Delete,
    }

    /// Workflow entity a change pertains to.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CdcEntity {
        Execution,
        Event,
        Definition,
    }

    /// A single change-data-capture event.
    #[derive(Debug, Clone)]
    pub struct CdcEvent {
        pub change_id: String,
        pub op: CdcOp,
        pub entity: CdcEntity,
        pub workflow_id: String,
        pub workflow_definition_id: Option<String>,
        pub run_id: Option<String>,
        pub event_id: Option<u64>,
        pub occurred_at: Option<chrono::DateTime<chrono::Utc>>,
        pub payload: serde_json::Value,
    }

    /// Filter selecting which changes to receive.
    #[derive(Debug, Clone, Default)]
    pub struct CdcFilter {
        pub workflow_ids: Vec<String>,
        pub workflow_definition_ids: Vec<String>,
        pub entities: Vec<CdcEntity>,
        pub resume_after: Option<String>,
    }

    /// A change-data-capture subscription source.
    #[async_trait::async_trait]
    pub trait FluxCdcClient: Send + Sync {
        /// Open a subscription stream of [`CdcEvent`]s. The returned receiver is
        /// fed by a background task until the underlying stream ends.
        async fn subscribe(&self, filter: CdcFilter) -> Result<mpsc::Receiver<CdcEvent>>;
    }

    /// gRPC Flux CDC client backed by a tonic channel.
    pub struct GrpcFluxCdcClient {
        inner: FluxClient<tonic::transport::Channel>,
    }

    impl GrpcFluxCdcClient {
        pub fn new(channel: tonic::transport::Channel) -> Self {
            Self {
                inner: FluxClient::new(channel),
            }
        }

        pub async fn connect(url: &str) -> Result<Self> {
            let endpoint = tonic::transport::Endpoint::from_shared(url.to_string())
                .map_err(|e| Error::Transport(Box::new(e)))?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| Error::Transport(Box::new(e)))?;
            Ok(Self::new(channel))
        }
    }

    #[async_trait::async_trait]
    impl FluxCdcClient for GrpcFluxCdcClient {
        async fn subscribe(&self, filter: CdcFilter) -> Result<mpsc::Receiver<CdcEvent>> {
            let req = FluxChangeSubscription {
                workflow_ids: filter.workflow_ids,
                workflow_definition_ids: filter.workflow_definition_ids,
                entities: filter.entities.iter().map(entity_to_proto).collect(),
                resume_after: filter.resume_after.unwrap_or_default(),
            };

            let mut stream = self
                .inner
                .clone()
                .subscribe(req)
                .await
                .map_err(|e| Error::Grpc(Box::new(e)))?
                .into_inner();

            let (tx, rx) = mpsc::channel(256);
            tokio::spawn(async move {
                while let Ok(Some(proto)) = stream.message().await {
                    match map_event(proto) {
                        Ok(ev) => {
                            if tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => warn!("flux cdc: dropping malformed event: {e}"),
                    }
                }
            });
            Ok(rx)
        }
    }

    fn entity_to_proto(e: &CdcEntity) -> i32 {
        let kind = match e {
            CdcEntity::Execution => FluxEntityKind::Execution,
            CdcEntity::Event => FluxEntityKind::Event,
            CdcEntity::Definition => FluxEntityKind::Definition,
        };
        kind as i32
    }

    fn map_event(p: ProtoEvent) -> Result<CdcEvent> {
        let op = match FluxChangeOp::try_from(p.op) {
            Ok(FluxChangeOp::Insert) => CdcOp::Insert,
            Ok(FluxChangeOp::Update) => CdcOp::Update,
            Ok(FluxChangeOp::Delete) => CdcOp::Delete,
            _ => CdcOp::Insert,
        };
        let entity = match FluxEntityKind::try_from(p.entity) {
            Ok(FluxEntityKind::Execution) => CdcEntity::Execution,
            Ok(FluxEntityKind::Event) => CdcEntity::Event,
            Ok(FluxEntityKind::Definition) => CdcEntity::Definition,
            _ => CdcEntity::Execution,
        };
        let occurred_at = p.occurred_at.and_then(|t| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(t.seconds.max(0), t.nanos.max(0) as u32)
        });
        Ok(CdcEvent {
            change_id: p.change_id,
            op,
            entity,
            workflow_id: p.workflow_id,
            workflow_definition_id: if p.workflow_definition_id.is_empty() {
                None
            } else {
                Some(p.workflow_definition_id)
            },
            run_id: if p.run_id.is_empty() {
                None
            } else {
                Some(p.run_id)
            },
            event_id: if p.event_id == 0 {
                None
            } else {
                Some(p.event_id as u64)
            },
            occurred_at,
            payload: p
                .payload
                .map(struct_to_json)
                .unwrap_or(serde_json::Value::Null),
        })
    }

    fn struct_to_json(s: Struct) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (k, v) in s.fields {
            m.insert(k, value_to_json(v));
        }
        serde_json::Value::Object(m)
    }

    fn value_to_json(v: prost_types::Value) -> serde_json::Value {
        match v.kind {
            Some(Kind::NullValue(_)) => serde_json::Value::Null,
            Some(Kind::NumberValue(n)) => serde_json::json!(n),
            Some(Kind::BoolValue(b)) => serde_json::Value::Bool(b),
            Some(Kind::StringValue(s)) => serde_json::Value::String(s),
            Some(Kind::StructValue(s)) => struct_to_json(s),
            Some(Kind::ListValue(l)) => {
                serde_json::Value::Array(l.values.into_iter().map(value_to_json).collect())
            }
            None => serde_json::Value::Null,
        }
    }
}

#[cfg(feature = "flux")]
pub use generated::*;
