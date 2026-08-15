//! Integration tests for the Phase 5 synapse task-queue transport.
//!
//! These stand up an in-process mock of the tpt-synapse `MatchingService` (the
//! queue+ack primitive) and exercise [`SynapseTaskQueue`] (dispatch) and
//! [`SynapseTaskCompletionListener`] (async completion listening with a consumer
//! group) against it — without depending on the cross-repo tpt-synapse server.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};
use tpt_workflow_core::synapse::{
    ConsumerGroup, ConsumerGroupRegistry, SynapseTaskCompletionListener, SynapseTaskQueue,
    SynapseTaskQueueConfig, TaskHandler,
};
use tpt_workflow_core::task_queue::{QueuedTask, TaskQueue, TaskStatus};
use tpt_workflow_core::workflow::{StepId, TaskId, WorkflowExecutionId};
use tpt_workflow_proto::synapse::workflow::v1::matching_service_server::{
    MatchingService, MatchingServiceServer,
};
use tpt_workflow_proto::synapse::workflow::v1::{
    AddTaskRequest, AddTaskResponse, PollTaskRequest, PollTaskResponse, RespondRequest,
    RespondResponse, StreamTasksRequest, Task,
};

#[derive(Default)]
struct MockInner {
    queue: Mutex<VecDeque<(Vec<u8>, Vec<u8>)>>, // (payload, token)
    inflight: Mutex<HashMap<Vec<u8>, Vec<u8>>>, // token -> payload
    notify: Notify,
}

#[derive(Clone)]
struct MockMatching {
    inner: Arc<MockInner>,
}

#[tonic::async_trait]
impl MatchingService for MockMatching {
    async fn add_task(
        &self,
        request: Request<AddTaskRequest>,
    ) -> Result<Response<AddTaskResponse>, Status> {
        let r = request.into_inner();
        let token = uuid::Uuid::new_v4().into_bytes().to_vec();
        self.inner
            .queue
            .lock()
            .await
            .push_back((r.payload, token.clone()));
        self.inner.notify.notify_waiters();
        Ok(Response::new(AddTaskResponse { seq: 1 }))
    }

    async fn poll_task(
        &self,
        _request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        let item = self.inner.queue.lock().await.pop_front();
        match item {
            Some((payload, token)) => {
                self.inner
                    .inflight
                    .lock()
                    .await
                    .insert(token.clone(), payload.clone());
                Ok(Response::new(PollTaskResponse {
                    task: Some(Task {
                        task_token: token,
                        task_queue: String::new(),
                        task_type: String::new(),
                        payload,
                        attempt: 1,
                    }),
                }))
            }
            None => Ok(Response::new(PollTaskResponse { task: None })),
        }
    }

    type StreamTasksStream = ReceiverStream<Result<Task, Status>>;

    async fn stream_tasks(
        &self,
        _request: Request<StreamTasksRequest>,
    ) -> Result<Response<Self::StreamTasksStream>, Status> {
        let inner = self.inner.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            loop {
                let item = inner.queue.lock().await.pop_front();
                match item {
                    Some((payload, token)) => {
                        inner
                            .inflight
                            .lock()
                            .await
                            .insert(token.clone(), payload.clone());
                        let task = Task {
                            task_token: token,
                            task_queue: String::new(),
                            task_type: String::new(),
                            payload,
                            attempt: 1,
                        };
                        if tx.send(Ok(task)).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = inner.notify.notified() => {}
                            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                        }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn respond_task_completed(
        &self,
        request: Request<RespondRequest>,
    ) -> Result<Response<RespondResponse>, Status> {
        let token = request.into_inner().task_token;
        let accepted = self.inner.inflight.lock().await.remove(&token).is_some();
        Ok(Response::new(RespondResponse {
            accepted,
            duplicate: false,
        }))
    }

    async fn respond_task_failed(
        &self,
        request: Request<RespondRequest>,
    ) -> Result<Response<RespondResponse>, Status> {
        let r = request.into_inner();
        let accepted = match self.inner.inflight.lock().await.remove(&r.task_token) {
            Some(payload) => {
                self.inner
                    .queue
                    .lock()
                    .await
                    .push_back((payload, r.task_token.clone()));
                self.inner.notify.notify_waiters();
                true
            }
            None => false,
        };
        Ok(Response::new(RespondResponse {
            accepted,
            duplicate: false,
        }))
    }
}

async fn start_mock() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let svc = MockMatching {
        inner: Arc::new(MockInner::default()),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MatchingServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

fn make_task(token: &str) -> QueuedTask {
    QueuedTask {
        task_id: TaskId::from_string("t1".to_string()),
        task_type: "activity".to_string(),
        task_queue: "default".to_string(),
        input: serde_json::json!({"x": 1}),
        workflow_id: WorkflowExecutionId::from_string("run-1".to_string()),
        step_id: StepId::from_string("s1".to_string()),
        attempt: 1,
        status: TaskStatus::Pending,
        scheduled_at: chrono::Utc::now(),
        started_at: None,
        completed_at: None,
        timeout_policy: Default::default(),
        retry_policy: Default::default(),
        task_token: token.to_string(),
        permission: Default::default(),
    }
}

#[tokio::test]
async fn synapse_queue_enqueue_dequeue_complete() {
    let endpoint = start_mock().await;
    let queue = SynapseTaskQueue::connect(SynapseTaskQueueConfig {
        endpoint,
        tenant: "t".to_string(),
        task_queue: "q1".to_string(),
        task_type: None,
        visibility_timeout: Duration::from_secs(30),
        long_poll_timeout: Duration::from_millis(100),
    })
    .unwrap();

    let task = make_task("tok-1");
    queue.enqueue(task.clone()).await.unwrap();

    let got = queue.dequeue("w1", 1).await.unwrap();
    assert_eq!(got.len(), 1, "exactly one task should be dequeued");
    assert_eq!(got[0].task_token, "tok-1");

    // start_task should promote the local registry entry to Running.
    queue.start_task("tok-1", "w1").await.unwrap();
    assert_eq!(
        queue.get_task("tok-1").await.unwrap().unwrap().status,
        TaskStatus::Running
    );

    queue
        .complete_task("tok-1", serde_json::json!({"ok": true}))
        .await
        .unwrap();

    let stats = queue.get_stats().await.unwrap();
    assert_eq!(stats.completed_tasks, 1);
    assert_eq!(stats.running_tasks, 0);
    assert!(queue.get_task("tok-1").await.unwrap().is_none());
}

#[tokio::test]
async fn synapse_queue_fail_redelivers() {
    let endpoint = start_mock().await;
    let queue = SynapseTaskQueue::connect(SynapseTaskQueueConfig {
        endpoint,
        tenant: "t".to_string(),
        task_queue: "qf".to_string(),
        task_type: None,
        visibility_timeout: Duration::from_secs(30),
        long_poll_timeout: Duration::from_millis(100),
    })
    .unwrap();

    queue.enqueue(make_task("tok-f")).await.unwrap();
    let got = queue.dequeue("w", 1).await.unwrap();
    assert_eq!(got.len(), 1);

    // Failing the task nacks it in synapse; a subsequent dequeue redelivers it.
    queue.fail_task("tok-f", "boom".to_string()).await.unwrap();
    let stats = queue.get_stats().await.unwrap();
    assert_eq!(stats.failed_tasks, 1);

    let again = queue.dequeue("w", 1).await.unwrap();
    assert_eq!(again.len(), 1, "failed task should be redelivered");
    assert_eq!(again[0].task_token, "tok-f");
}

struct CaptureHandler {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl TaskHandler for CaptureHandler {
    async fn handle(&self, task: QueuedTask) -> std::result::Result<serde_json::Value, String> {
        self.tx.send(task.task_token).await.unwrap();
        Ok(serde_json::json!({"handled": true}))
    }
}

#[tokio::test]
async fn synapse_completion_listener_consumes_and_acks() {
    let endpoint = start_mock().await;
    let registry = ConsumerGroupRegistry::new();

    let (tx, mut rx) = mpsc::channel(8);
    let listener = SynapseTaskCompletionListener::connect(
        ConsumerGroup {
            tenant: "t".to_string(),
            task_queue: "q2".to_string(),
            task_type: String::new(),
            group: "cg-a".to_string(),
        },
        &endpoint,
        Arc::new(CaptureHandler { tx }),
        Duration::from_secs(30),
    )
    .unwrap()
    .with_registry(registry.clone());

    listener.start().await.unwrap();
    assert!(listener.is_running());
    assert_eq!(registry.member_count("cg-a").await, 1);

    let queue = SynapseTaskQueue::connect(SynapseTaskQueueConfig {
        endpoint,
        tenant: "t".to_string(),
        task_queue: "q2".to_string(),
        task_type: None,
        visibility_timeout: Duration::from_secs(30),
        long_poll_timeout: Duration::from_millis(100),
    })
    .unwrap();

    queue.enqueue(make_task("tok-l")).await.unwrap();

    // The listener should pull and ack the task.
    let consumed = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("listener should consume the task")
        .unwrap();
    assert_eq!(consumed, "tok-l");

    // After acking, the task must not be redelivered.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = queue.dequeue("w", 1).await.unwrap();
    assert!(after.is_empty(), "acked task must not be redelivered");

    listener.stop().await;
    assert!(!listener.is_running());
    assert_eq!(registry.member_count("cg-a").await, 0);
}

#[tokio::test]
async fn synapse_cancel_is_not_supported() {
    let endpoint = start_mock().await;
    let queue = SynapseTaskQueue::connect(SynapseTaskQueueConfig {
        endpoint,
        tenant: "t".to_string(),
        task_queue: "qc".to_string(),
        task_type: None,
        visibility_timeout: Duration::from_secs(30),
        long_poll_timeout: Duration::from_millis(100),
    })
    .unwrap();

    queue.enqueue(make_task("tok-c")).await.unwrap();
    let got = queue.dequeue("w", 1).await.unwrap();
    assert_eq!(got.len(), 1);

    // The queue+ack primitive has no hard-delete, so cancel is unsupported.
    let err = queue.cancel_task("tok-c", "user abort".to_string()).await;
    assert!(err.is_err());
    assert!(format!("{err:?}").contains("NotImplemented"));
}
