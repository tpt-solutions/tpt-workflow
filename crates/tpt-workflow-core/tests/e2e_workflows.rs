use async_trait::async_trait;
use tpt_workflow_core::dev_harness::DevEnvironment;
use tpt_workflow_core::permissions::{
    ActorContext, ActorStore, AuthDecision, AuthorizationCheck, CompositeAuth, RoleBasedAuth,
    StepPermissionChecker,
};
use tpt_workflow_core::persistence::PersistenceLayer;
use tpt_workflow_core::workflow::{StepDefinition, StepId, StepType, WorkflowDefinition};

fn build_single_task_workflow() -> WorkflowDefinition {
    let step_id = StepId::from_string("step-1".to_string());
    let step = StepDefinition::task(
        step_id,
        "process-data".to_string(),
        "default".to_string(),
        "process".to_string(),
    );
    WorkflowDefinition::new("test-workflow".to_string(), "1.0.0".to_string()).add_step(step)
}

fn build_permitted_task_workflow() -> WorkflowDefinition {
    let step_id = StepId::from_string("step-1".to_string());
    let mut step = StepDefinition::task(
        step_id,
        "process-data".to_string(),
        "default".to_string(),
        "process".to_string(),
    );
    step.permission = tpt_workflow_core::workflow::PermissionRequirement {
        required_role: Some("admin".to_string()),
        required_attributes: vec![],
        required_level: None,
    };
    WorkflowDefinition::new("test-workflow".to_string(), "1.0.0".to_string()).add_step(step)
}

fn build_two_step_workflow() -> WorkflowDefinition {
    let step1_id = StepId::from_string("step-1".to_string());
    let step2_id = StepId::from_string("step-2".to_string());

    let step1 = StepDefinition::task(
        step1_id.clone(),
        "fetch-data".to_string(),
        "default".to_string(),
        "fetch".to_string(),
    );

    let step2 = StepDefinition::task(
        step2_id.clone(),
        "process-data".to_string(),
        "default".to_string(),
        "process".to_string(),
    )
    .depends_on(vec![step1_id]);

    WorkflowDefinition::new("multi-step-workflow".to_string(), "1.0.0".to_string())
        .add_step(step1)
        .add_step(step2)
}

fn build_decision_workflow() -> WorkflowDefinition {
    let step1_id = StepId::from_string("step-1".to_string());
    let step1 = StepDefinition::task(
        step1_id,
        "evaluate".to_string(),
        "default".to_string(),
        "evaluate".to_string(),
    );

    let mut step2 = StepDefinition::new(
        StepId::from_string("step-2".to_string()),
        "branch-a".to_string(),
        StepType::Decision,
    );
    step2.task_queue = "default".to_string();

    WorkflowDefinition::new("decision-workflow".to_string(), "1.0.0".to_string())
        .add_step(step1)
        .add_step(step2)
}

#[tokio::test]
async fn test_single_task_workflow_completes() {
    let harness = DevEnvironment::new().register_task_handler("process", |input| {
        let name = input
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        serde_json::json!({ "result": format!("processed-{}", name) })
    });

    let definition = build_single_task_workflow();
    let input = serde_json::json!({ "name": "test-data" });

    let execution = harness.run_to_completion(definition, input).await.unwrap();

    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
}

#[tokio::test]
async fn test_role_based_auth_allows_matching_role() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["admin".to_string(), "manager".to_string()],
        attributes: vec![],
        clearance_level: 5,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_role = Some("admin".to_string());

    let result = auth.check_permission(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_role_based_auth_denies_missing_role() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["viewer".to_string()],
        attributes: vec![],
        clearance_level: 5,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_role = Some("admin".to_string());

    let result = auth.check_permission(&step, &actor).await.unwrap();
    match result {
        AuthDecision::Deny(reason) => assert!(reason.contains("Missing required role")),
        AuthDecision::Allow => panic!("Expected denial"),
    }
}

#[tokio::test]
async fn test_role_based_auth_allows_matching_attributes() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec!["finance".to_string(), "level-3".to_string()],
        clearance_level: 5,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_attributes = vec!["finance".to_string()];

    let result = auth.check_permission(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_role_based_auth_denies_missing_attribute() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec!["engineering".to_string()],
        clearance_level: 5,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_attributes = vec!["finance".to_string()];

    let result = auth.check_permission(&step, &actor).await.unwrap();
    match result {
        AuthDecision::Deny(reason) => assert!(reason.contains("Missing required attribute")),
        AuthDecision::Allow => panic!("Expected denial"),
    }
}

#[tokio::test]
async fn test_role_based_auth_allows_sufficient_clearance() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 10,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_level = Some(5);

    let result = auth.check_permission(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_role_based_auth_denies_insufficient_clearance() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 2,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_level = Some(5);

    let result = auth.check_permission(&step, &actor).await.unwrap();
    match result {
        AuthDecision::Deny(reason) => assert!(reason.contains("Insufficient clearance")),
        AuthDecision::Allow => panic!("Expected denial"),
    }
}

#[tokio::test]
async fn test_role_based_auth_combined_requirements() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["admin".to_string()],
        attributes: vec!["finance".to_string()],
        clearance_level: 10,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_role = Some("admin".to_string());
    step.permission.required_attributes = vec!["finance".to_string()];
    step.permission.required_level = Some(5);

    let result = auth.check_permission(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_role_based_auth_combined_fails_on_one() {
    use std::sync::Arc;

    let (auth, store) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["admin".to_string()],
        attributes: vec!["finance".to_string()],
        clearance_level: 10,
    };
    store.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_role = Some("admin".to_string());
    step.permission.required_attributes = vec!["engineering".to_string()];
    step.permission.required_level = Some(5);

    let result = auth.check_permission(&step, &actor).await.unwrap();
    match result {
        AuthDecision::Deny(reason) => assert!(reason.contains("Missing required attribute")),
        AuthDecision::Allow => panic!("Expected denial"),
    }
}

#[tokio::test]
async fn test_composite_auth_allows_when_all_pass() {
    use std::sync::Arc;

    let (auth1, _store1) = RoleBasedAuth::with_memory_store();
    let (auth2, _store2) = RoleBasedAuth::with_memory_store();

    let composite = CompositeAuth::new()
        .add_checker(Box::new(auth1))
        .add_checker(Box::new(auth2));

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };

    let step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );

    let result = composite.check_permission(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_composite_auth_denies_when_any_fails() {
    use std::sync::Arc;

    let (auth1, store1) = RoleBasedAuth::with_memory_store();
    let (auth2, store2) = RoleBasedAuth::with_memory_store();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["admin".to_string()],
        attributes: vec![],
        clearance_level: 0,
    };
    store1.upsert_actor(actor.clone()).await;
    store2.upsert_actor(actor.clone()).await;

    let mut step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    step.permission.required_role = Some("admin".to_string());

    let step_no_role = StepDefinition::task(
        StepId::from_string("test2".to_string()),
        "test2".to_string(),
        "default".to_string(),
        "test2".to_string(),
    );

    let composite = CompositeAuth::new()
        .add_checker(Box::new(auth1))
        .add_checker(Box::new(auth2));

    let result = composite
        .check_permission(&step_no_role, &actor)
        .await
        .unwrap();
    assert_eq!(result, AuthDecision::Allow);

    let mut step_denied = StepDefinition::task(
        StepId::from_string("test3".to_string()),
        "test3".to_string(),
        "default".to_string(),
        "test3".to_string(),
    );
    step_denied.permission.required_role = Some("superadmin".to_string());

    let result = composite
        .check_permission(&step_denied, &actor)
        .await
        .unwrap();
    match result {
        AuthDecision::Deny(_) => {}
        AuthDecision::Allow => panic!("Expected denial"),
    }
}

#[tokio::test]
async fn test_actor_store_upsert_and_get() {
    let store = ActorStore::new();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec!["admin".to_string()],
        attributes: vec!["finance".to_string()],
        clearance_level: 5,
    };
    store.upsert_actor(actor.clone()).await;

    let retrieved = store.get_actor("user-1").await;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.actor_id, "user-1");
    assert_eq!(retrieved.roles, vec!["admin".to_string()]);
    assert_eq!(retrieved.attributes, vec!["finance".to_string()]);
    assert_eq!(retrieved.clearance_level, 5);
}

#[tokio::test]
async fn test_actor_store_add_remove_role() {
    let store = ActorStore::new();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };
    store.upsert_actor(actor).await;

    store.add_role("user-1", "admin").await.unwrap();
    assert!(store.has_role("user-1", "admin").await);

    store.add_role("user-1", "manager").await.unwrap();
    assert!(store.has_role("user-1", "manager").await);

    store.remove_role("user-1", "admin").await.unwrap();
    assert!(!store.has_role("user-1", "admin").await);
    assert!(store.has_role("user-1", "manager").await);
}

#[tokio::test]
async fn test_actor_store_add_remove_attribute() {
    let store = ActorStore::new();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };
    store.upsert_actor(actor).await;

    store.add_attribute("user-1", "finance").await.unwrap();
    assert!(store.has_attribute("user-1", "finance").await);

    store.add_attribute("user-1", "level-3").await.unwrap();
    assert!(store.has_attribute("user-1", "level-3").await);

    store.remove_attribute("user-1", "finance").await.unwrap();
    assert!(!store.has_attribute("user-1", "finance").await);
    assert!(store.has_attribute("user-1", "level-3").await);
}

#[tokio::test]
async fn test_actor_store_set_clearance_level() {
    let store = ActorStore::new();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };
    store.upsert_actor(actor).await;

    store.set_clearance_level("user-1", 10).await.unwrap();
    let retrieved = store.get_actor("user-1").await.unwrap();
    assert_eq!(retrieved.clearance_level, 10);
}

#[tokio::test]
async fn test_actor_store_remove_actor() {
    let store = ActorStore::new();

    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };
    store.upsert_actor(actor).await;
    assert!(store.get_actor("user-1").await.is_some());

    let removed = store.remove_actor("user-1").await;
    assert!(removed);
    assert!(store.get_actor("user-1").await.is_none());
}

#[tokio::test]
async fn test_actor_store_list_actors() {
    let store = ActorStore::new();

    for i in 0..5 {
        let actor = ActorContext {
            actor_id: format!("user-{}", i),
            roles: vec![],
            attributes: vec![],
            clearance_level: 0,
        };
        store.upsert_actor(actor).await;
    }

    let actors = store.list_actors().await;
    assert_eq!(actors.len(), 5);
}

#[tokio::test]
async fn test_role_based_auth_with_store_lookups() {
    let (auth, store) = RoleBasedAuth::with_memory_store();

    store
        .upsert_actor(ActorContext {
            actor_id: "admin-user".to_string(),
            roles: vec!["admin".to_string(), "finance".to_string()],
            attributes: vec!["clearance-5".to_string()],
            clearance_level: 5,
        })
        .await;

    store
        .upsert_actor(ActorContext {
            actor_id: "regular-user".to_string(),
            roles: vec!["viewer".to_string()],
            attributes: vec!["clearance-1".to_string()],
            clearance_level: 1,
        })
        .await;

    let mut admin_step = StepDefinition::task(
        StepId::from_string("admin-step".to_string()),
        "admin-step".to_string(),
        "default".to_string(),
        "admin-task".to_string(),
    );
    admin_step.permission.required_role = Some("admin".to_string());
    admin_step.permission.required_level = Some(3);

    let admin_actor = store.get_actor("admin-user").await.unwrap();
    let result = auth
        .check_permission(&admin_step, &admin_actor)
        .await
        .unwrap();
    assert_eq!(result, AuthDecision::Allow);

    let regular_actor = store.get_actor("regular-user").await.unwrap();
    let result = auth
        .check_permission(&admin_step, &regular_actor)
        .await
        .unwrap();
    match result {
        AuthDecision::Deny(reason) => assert!(
            reason.contains("Missing required role") || reason.contains("Insufficient clearance")
        ),
        AuthDecision::Allow => panic!("Expected denial for regular user on admin step"),
    }
}

#[tokio::test]
async fn test_two_step_workflow_sequential() {
    let harness = DevEnvironment::new()
        .register_task_handler("fetch", |_input| serde_json::json!({ "data": [1, 2, 3] }))
        .register_task_handler("process", |input| {
            let data = input
                .get("step_step-1")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            serde_json::json!({ "result": "processed", "input_data": data })
        });

    let definition = build_two_step_workflow();
    let input = serde_json::json!({ "query": "test" });

    let execution = harness.run_to_completion(definition, input).await.unwrap();

    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
    assert_eq!(execution.output["result"], "processed");
}

#[tokio::test]
async fn test_workflow_with_missing_handler_fails() {
    let harness = DevEnvironment::new();

    let definition = build_single_task_workflow();
    let input = serde_json::json!({});

    let result = harness.run_to_completion(definition, input).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_permission_check_allows_workflows() {
    let harness = DevEnvironment::new()
        .register_task_handler("process", |_input| serde_json::json!({ "ok": true }));

    let definition = build_single_task_workflow();
    let input = serde_json::json!({});

    let execution = harness.run_to_completion(definition, input).await.unwrap();
    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
}

struct DenyAllAuth;

#[async_trait]
impl AuthorizationCheck for DenyAllAuth {
    async fn check_permission(
        &self,
        _step: &StepDefinition,
        _actor: &ActorContext,
    ) -> tpt_workflow_core::Result<AuthDecision> {
        Ok(AuthDecision::Deny("test: access denied".to_string()))
    }
}

#[tokio::test]
async fn test_permission_denial_blocks_step() {
    use std::sync::Arc;

    let persistence = Arc::new(tpt_workflow_core::persistence::InMemoryPersistence::new());
    let task_queue_manager = Arc::new(tpt_workflow_core::TaskQueueManager::new());
    let worker_registry = Arc::new(tpt_workflow_core::worker::InMemoryWorkerRegistry::new());

    let queue = std::sync::Arc::new(tpt_workflow_core::task_queue::InMemoryTaskQueue::new(
        "default".to_string(),
    ));
    task_queue_manager.register_queue(queue).await.unwrap();

    let executor = tpt_workflow_core::WorkflowExecutor::new(
        persistence.clone(),
        task_queue_manager.clone(),
        worker_registry.clone(),
    )
    .with_permissions(StepPermissionChecker::new(Box::new(DenyAllAuth)));

    let definition = build_permitted_task_workflow();
    let def = Arc::new(definition);
    let input = serde_json::json!({});
    let workflow_id = tpt_workflow_core::workflow::WorkflowExecutionId::new();

    persistence.save_workflow_definition(&def).await.unwrap();

    let mut execution = tpt_workflow_core::WorkflowExecution::new(
        workflow_id.clone(),
        def.id.clone(),
        def.version.clone(),
        input.clone(),
    );
    execution.status = tpt_workflow_core::WorkflowExecutionStatus::Running;

    let mut context = tpt_workflow_core::ExecutionContext::new(workflow_id, def.clone(), input);

    let result = executor
        .execute_workflow(&mut execution, &mut context)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Permission denied"),
        "Expected permission denied error, got: {}",
        err
    );
}

#[tokio::test]
async fn test_allow_all_permission_passes() {
    let checker = StepPermissionChecker::allow_all();
    let step = StepDefinition::task(
        StepId::from_string("test".to_string()),
        "test".to_string(),
        "default".to_string(),
        "test".to_string(),
    );
    let actor = ActorContext {
        actor_id: "user-1".to_string(),
        roles: vec![],
        attributes: vec![],
        clearance_level: 0,
    };

    let result = checker.check(&step, &actor).await.unwrap();
    assert_eq!(result, AuthDecision::Allow);
}

#[tokio::test]
async fn test_workflow_execution_persists() {
    let harness = DevEnvironment::new()
        .register_task_handler("process", |_input| serde_json::json!({ "done": true }));

    let definition = build_single_task_workflow();
    let input = serde_json::json!({});

    let execution = harness.run_to_completion(definition, input).await.unwrap();
    let workflow_id = execution.workflow_id.clone();

    let saved = harness.get_execution(&workflow_id).await.unwrap();
    assert!(saved.is_some());
    let saved = saved.unwrap();
    assert_eq!(
        saved.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
    assert_eq!(saved.output["done"], true);
}

#[tokio::test]
async fn test_empty_workflow_definition_validation() {
    let definition = WorkflowDefinition::new("empty".to_string(), "1.0.0".to_string());
    let result = definition.validate();
    assert!(result.is_err());
}

#[tokio::test]
async fn test_workflow_with_custom_retry_policy() {
    let step_id = StepId::from_string("retry-step".to_string());
    let mut step = StepDefinition::task(
        step_id,
        "retry-task".to_string(),
        "default".to_string(),
        "retryable".to_string(),
    );
    step.retry_policy.max_attempts = 2;

    let definition =
        WorkflowDefinition::new("retry-workflow".to_string(), "1.0.0".to_string()).add_step(step);

    let harness = DevEnvironment::new().register_task_handler(
        "retryable",
        |_input| serde_json::json!({ "attempted": true }),
    );

    let execution = harness
        .run_to_completion(definition, serde_json::json!({}))
        .await
        .unwrap();

    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
}

#[tokio::test]
async fn test_crash_recovery_resume_from_durable_state() {
    use std::sync::Arc;
    use tpt_workflow_core::persistence::InMemoryPersistence;
    use tpt_workflow_core::task_queue::{InMemoryTaskQueue, TaskQueueManager};
    use tpt_workflow_core::worker::InMemoryWorkerRegistry;
    use tpt_workflow_core::workflow::WorkflowExecution;

    let persistence = Arc::new(InMemoryPersistence::new());
    let task_queue_manager = Arc::new(TaskQueueManager::new());
    let worker_registry = Arc::new(InMemoryWorkerRegistry::new());

    let queue = Arc::new(InMemoryTaskQueue::new("default".to_string()));
    task_queue_manager.register_queue(queue).await.unwrap();

    let executor = Arc::new(tpt_workflow_core::WorkflowExecutor::new(
        persistence.clone(),
        task_queue_manager.clone(),
        worker_registry.clone(),
    ));

    let step1_id = StepId::from_string("step-1".to_string());
    let step2_id = StepId::from_string("step-2".to_string());
    let step3_id = StepId::from_string("step-3".to_string());

    let step1 = StepDefinition::task(
        step1_id.clone(),
        "fetch-data".to_string(),
        "default".to_string(),
        "fetch".to_string(),
    );
    let step2 = StepDefinition::task(
        step2_id.clone(),
        "process-data".to_string(),
        "default".to_string(),
        "process".to_string(),
    )
    .depends_on(vec![step1_id.clone()]);
    let step3 = StepDefinition::task(
        step3_id.clone(),
        "write-output".to_string(),
        "default".to_string(),
        "write".to_string(),
    )
    .depends_on(vec![step2_id.clone()]);

    let definition = Arc::new(
        WorkflowDefinition::new("crash-recovery-workflow".to_string(), "1.0.0".to_string())
            .add_step(step1)
            .add_step(step2)
            .add_step(step3),
    );

    persistence
        .save_workflow_definition(&definition)
        .await
        .unwrap();

    let workflow_id = tpt_workflow_core::workflow::WorkflowExecutionId::new();
    let input = serde_json::json!({ "request_id": "crash-test-123" });

    let mut execution = WorkflowExecution::new(
        workflow_id.clone(),
        definition.id.clone(),
        definition.version.clone(),
        input.clone(),
    );
    execution.status = tpt_workflow_core::WorkflowExecutionStatus::Running;

    let mut context = tpt_workflow_core::ExecutionContext::new(
        workflow_id.clone(),
        definition.clone(),
        input.clone(),
    );

    let result = executor
        .execute_workflow(&mut execution, &mut context)
        .await;
    assert!(result.is_ok());

    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Running
    );
    assert!(execution.current_step.is_some());

    let current_step = execution.current_step.clone().unwrap();
    assert_eq!(current_step, step1_id);

    persistence
        .save_workflow_execution(&execution)
        .await
        .unwrap();

    let pending_tasks = task_queue_manager
        .poll_tasks("dev-worker", &["default".to_string()], 10)
        .await
        .unwrap();
    assert_eq!(pending_tasks.len(), 1);
    assert_eq!(pending_tasks[0].task_type, "fetch");

    let queue = task_queue_manager.get_queue("default").await.unwrap();
    queue
        .complete_task(
            &pending_tasks[0].task_token,
            serde_json::json!({ "data": [1, 2, 3] }),
        )
        .await
        .unwrap();

    context.set_step_output(step1_id.clone(), serde_json::json!({ "data": [1, 2, 3] }));

    let result = executor
        .execute_workflow(&mut execution, &mut context)
        .await;
    assert!(result.is_ok());

    persistence
        .save_workflow_execution(&execution)
        .await
        .unwrap();

    let pending_tasks = task_queue_manager
        .poll_tasks("dev-worker", &["default".to_string()], 10)
        .await
        .unwrap();
    assert_eq!(pending_tasks.len(), 1);
    assert_eq!(pending_tasks[0].task_type, "process");

    queue
        .complete_task(
            &pending_tasks[0].task_token,
            serde_json::json!({ "processed": true }),
        )
        .await
        .unwrap();

    context.set_step_output(step2_id.clone(), serde_json::json!({ "processed": true }));

    let result = executor
        .execute_workflow(&mut execution, &mut context)
        .await;
    assert!(result.is_ok());

    persistence
        .save_workflow_execution(&execution)
        .await
        .unwrap();

    let pending_tasks = task_queue_manager
        .poll_tasks("dev-worker", &["default".to_string()], 10)
        .await
        .unwrap();
    assert_eq!(pending_tasks.len(), 1);
    assert_eq!(pending_tasks[0].task_type, "write");

    queue
        .complete_task(
            &pending_tasks[0].task_token,
            serde_json::json!({ "written": true }),
        )
        .await
        .unwrap();

    context.set_step_output(step3_id.clone(), serde_json::json!({ "written": true }));

    let result = executor
        .execute_workflow(&mut execution, &mut context)
        .await;
    assert!(result.is_ok());

    assert_eq!(
        execution.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );

    persistence
        .save_workflow_execution(&execution)
        .await
        .unwrap();

    let saved = persistence
        .get_workflow_execution(&workflow_id)
        .await
        .unwrap();
    assert!(saved.is_some());
    let saved = saved.unwrap();
    assert_eq!(
        saved.status,
        tpt_workflow_core::WorkflowExecutionStatus::Completed
    );
    assert!(saved.completed_at.is_some());
}

#[tokio::test]
async fn test_replay_from_event_reconstructs_state() {
    use std::sync::Arc;
    use tpt_workflow_core::persistence::InMemoryPersistence;
    use tpt_workflow_core::replay::{LocalReplayEngine, ReplayMode, ReplayRequest};
    use tpt_workflow_core::workflow::{WorkflowEvent, WorkflowEventType};

    let persistence = Arc::new(InMemoryPersistence::new());

    let step1_id = StepId::from_string("step-1".to_string());
    let step2_id = StepId::from_string("step-2".to_string());

    let step1 = StepDefinition::task(
        step1_id.clone(),
        "fetch-data".to_string(),
        "default".to_string(),
        "fetch".to_string(),
    );
    let step2 = StepDefinition::task(
        step2_id.clone(),
        "process-data".to_string(),
        "default".to_string(),
        "process".to_string(),
    )
    .depends_on(vec![step1_id.clone()]);

    let definition = Arc::new(
        WorkflowDefinition::new("replay-test-workflow".to_string(), "1.0.0".to_string())
            .add_step(step1)
            .add_step(step2),
    );

    let events = vec![
        WorkflowEvent {
            event_id: 1,
            event_type: WorkflowEventType::WorkflowStarted,
            timestamp: chrono::Utc::now(),
            step_id: None,
            task_id: None,
            attributes: serde_json::json!({
                "workflow_definition_id": definition.id.to_string(),
                "workflow_definition_version": definition.version,
            }),
        },
        WorkflowEvent {
            event_id: 2,
            event_type: WorkflowEventType::StepStarted,
            timestamp: chrono::Utc::now(),
            step_id: Some(step1_id.clone()),
            task_id: None,
            attributes: serde_json::json!({
                "step_id": step1_id.to_string(),
                "step_name": "fetch-data",
            }),
        },
        WorkflowEvent {
            event_id: 3,
            event_type: WorkflowEventType::StepCompleted,
            timestamp: chrono::Utc::now(),
            step_id: Some(step1_id.clone()),
            task_id: None,
            attributes: serde_json::json!({
                "step_id": step1_id.to_string(),
                "output": { "data": [1, 2, 3] },
            }),
        },
        WorkflowEvent {
            event_id: 4,
            event_type: WorkflowEventType::StepStarted,
            timestamp: chrono::Utc::now(),
            step_id: Some(step2_id.clone()),
            task_id: None,
            attributes: serde_json::json!({
                "step_id": step2_id.to_string(),
                "step_name": "process-data",
            }),
        },
        WorkflowEvent {
            event_id: 5,
            event_type: WorkflowEventType::StepCompleted,
            timestamp: chrono::Utc::now(),
            step_id: Some(step2_id.clone()),
            task_id: None,
            attributes: serde_json::json!({
                "step_id": step2_id.to_string(),
                "output": { "processed": true },
            }),
        },
        WorkflowEvent {
            event_id: 6,
            event_type: WorkflowEventType::WorkflowCompleted,
            timestamp: chrono::Utc::now(),
            step_id: None,
            task_id: None,
            attributes: serde_json::json!({
                "output": { "final": true },
            }),
        },
    ];

    let local_replay = LocalReplayEngine::new(definition.clone(), events);
    let context = local_replay.replay(None).await.unwrap();

    assert_eq!(
        context.get_step_output(&step1_id),
        Some(&serde_json::json!({ "data": [1, 2, 3] }))
    );
    assert_eq!(
        context.get_step_output(&step2_id),
        Some(&serde_json::json!({ "processed": true }))
    );
    assert_eq!(
        context.variables.get("output"),
        Some(&serde_json::json!({ "final": true }))
    );
}

#[tokio::test]
async fn test_replay_with_modified_input() {
    use std::sync::Arc;
    use tpt_workflow_core::replay::LocalReplayEngine;
    use tpt_workflow_core::workflow::{WorkflowEvent, WorkflowEventType};

    let step1_id = StepId::from_string("step-1".to_string());
    let step1 = StepDefinition::task(
        step1_id.clone(),
        "process".to_string(),
        "default".to_string(),
        "process".to_string(),
    );

    let definition = Arc::new(
        WorkflowDefinition::new("replay-modified".to_string(), "1.0.0".to_string()).add_step(step1),
    );

    let events = vec![
        WorkflowEvent {
            event_id: 1,
            event_type: WorkflowEventType::WorkflowStarted,
            timestamp: chrono::Utc::now(),
            step_id: None,
            task_id: None,
            attributes: serde_json::json!({ "input": { "original": true } }),
        },
        WorkflowEvent {
            event_id: 2,
            event_type: WorkflowEventType::StepCompleted,
            timestamp: chrono::Utc::now(),
            step_id: Some(step1_id.clone()),
            task_id: None,
            attributes: serde_json::json!({
                "step_id": step1_id.to_string(),
                "output": { "result": "original" },
            }),
        },
    ];

    let new_input = serde_json::json!({ "modified": true, "new_data": "test" });
    let local_replay = LocalReplayEngine::new(definition, events);
    let context = local_replay.replay(Some(new_input.clone())).await.unwrap();

    assert_eq!(context.input, new_input);
}
