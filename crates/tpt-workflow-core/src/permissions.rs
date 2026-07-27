use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::workflow::StepDefinition;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorContext {
    pub actor_id: String,
    pub roles: Vec<String>,
    pub attributes: Vec<String>,
    pub clearance_level: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny(String),
}

#[async_trait]
pub trait AuthorizationCheck: Send + Sync {
    async fn check_permission(
        &self,
        step: &StepDefinition,
        actor: &ActorContext,
    ) -> Result<AuthDecision>;
}

pub struct AllowAllAuth;

#[async_trait]
impl AuthorizationCheck for AllowAllAuth {
    async fn check_permission(
        &self,
        _step: &StepDefinition,
        _actor: &ActorContext,
    ) -> Result<AuthDecision> {
        Ok(AuthDecision::Allow)
    }
}

pub struct StepPermissionChecker {
    inner: Box<dyn AuthorizationCheck>,
}

impl StepPermissionChecker {
    pub fn new(checker: Box<dyn AuthorizationCheck>) -> Self {
        Self { inner: checker }
    }

    pub fn allow_all() -> Self {
        Self::new(Box::new(AllowAllAuth))
    }

    pub async fn check(&self, step: &StepDefinition, actor: &ActorContext) -> Result<AuthDecision> {
        if step.permission.required_role.is_none()
            && step.permission.required_attributes.is_empty()
            && step.permission.required_level.is_none()
        {
            return Ok(AuthDecision::Allow);
        }
        self.inner.check_permission(step, actor).await
    }
}

/// In-memory store for actor assignments (roles, attributes, clearance levels)
pub struct ActorStore {
    actors: Arc<RwLock<HashMap<String, ActorContext>>>,
}

impl ActorStore {
    pub fn new() -> Self {
        Self {
            actors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register or update an actor's assignments
    pub async fn upsert_actor(&self, actor: ActorContext) {
        let mut actors = self.actors.write().await;
        actors.insert(actor.actor_id.clone(), actor);
    }

    /// Get an actor by ID
    pub async fn get_actor(&self, actor_id: &str) -> Option<ActorContext> {
        let actors = self.actors.read().await;
        actors.get(actor_id).cloned()
    }

    /// Remove an actor
    pub async fn remove_actor(&self, actor_id: &str) -> bool {
        let mut actors = self.actors.write().await;
        actors.remove(actor_id).is_some()
    }

    /// List all actors
    pub async fn list_actors(&self) -> Vec<ActorContext> {
        let actors = self.actors.read().await;
        actors.values().cloned().collect()
    }

    /// Check if an actor has a specific role
    pub async fn has_role(&self, actor_id: &str, role: &str) -> bool {
        let actors = self.actors.read().await;
        actors
            .get(actor_id)
            .map(|a| a.roles.iter().any(|r| r == role))
            .unwrap_or(false)
    }

    /// Check if an actor has a specific attribute
    pub async fn has_attribute(&self, actor_id: &str, attribute: &str) -> bool {
        let actors = self.actors.read().await;
        actors
            .get(actor_id)
            .map(|a| a.attributes.iter().any(|attr| attr == attribute))
            .unwrap_or(false)
    }

    /// Add a role to an actor
    pub async fn add_role(&self, actor_id: &str, role: &str) -> Result<()> {
        let mut actors = self.actors.write().await;
        let actor = actors
            .get_mut(actor_id)
            .ok_or_else(|| crate::error::Error::WorkerNotFound(format!("Actor not found: {}", actor_id)))?;
        if !actor.roles.iter().any(|r| r == role) {
            actor.roles.push(role.to_string());
        }
        Ok(())
    }

    /// Remove a role from an actor
    pub async fn remove_role(&self, actor_id: &str, role: &str) -> Result<()> {
        let mut actors = self.actors.write().await;
        let actor = actors
            .get_mut(actor_id)
            .ok_or_else(|| crate::error::Error::WorkerNotFound(format!("Actor not found: {}", actor_id)))?;
        actor.roles.retain(|r| r != role);
        Ok(())
    }

    /// Add an attribute to an actor
    pub async fn add_attribute(&self, actor_id: &str, attribute: &str) -> Result<()> {
        let mut actors = self.actors.write().await;
        let actor = actors
            .get_mut(actor_id)
            .ok_or_else(|| crate::error::Error::WorkerNotFound(format!("Actor not found: {}", actor_id)))?;
        if !actor.attributes.iter().any(|a| a == attribute) {
            actor.attributes.push(attribute.to_string());
        }
        Ok(())
    }

    /// Remove an attribute from an actor
    pub async fn remove_attribute(&self, actor_id: &str, attribute: &str) -> Result<()> {
        let mut actors = self.actors.write().await;
        let actor = actors
            .get_mut(actor_id)
            .ok_or_else(|| crate::error::Error::WorkerNotFound(format!("Actor not found: {}", actor_id)))?;
        actor.attributes.retain(|a| a != attribute);
        Ok(())
    }

    /// Set an actor's clearance level
    pub async fn set_clearance_level(&self, actor_id: &str, level: i32) -> Result<()> {
        let mut actors = self.actors.write().await;
        let actor = actors
            .get_mut(actor_id)
            .ok_or_else(|| crate::error::Error::WorkerNotFound(format!("Actor not found: {}", actor_id)))?;
        actor.clearance_level = level;
        Ok(())
    }
}

impl Default for ActorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Role-based authorization implementation with attribute and clearance-level checks
pub struct RoleBasedAuth {
    actor_store: Arc<ActorStore>,
}

impl RoleBasedAuth {
    pub fn new(actor_store: Arc<ActorStore>) -> Self {
        Self { actor_store }
    }

    /// Create with a new in-memory actor store
    pub fn with_memory_store() -> (Self, Arc<ActorStore>) {
        let store = Arc::new(ActorStore::new());
        let auth = Self::new(store.clone());
        (auth, store)
    }
}

#[async_trait]
impl AuthorizationCheck for RoleBasedAuth {
    async fn check_permission(
        &self,
        step: &StepDefinition,
        actor: &ActorContext,
    ) -> Result<AuthDecision> {
        let permission = &step.permission;

        if let Some(ref required_role) = permission.required_role {
            let actor_roles: HashSet<&str> = actor.roles.iter().map(|s| s.as_str()).collect();
            if !actor_roles.contains(required_role.as_str()) {
                return Ok(AuthDecision::Deny(format!(
                    "Missing required role: {}",
                    required_role
                )));
            }
        }

        if !permission.required_attributes.is_empty() {
            let actor_attrs: HashSet<&str> = actor.attributes.iter().map(|s| s.as_str()).collect();
            for required_attr in &permission.required_attributes {
                if !actor_attrs.contains(required_attr.as_str()) {
                    return Ok(AuthDecision::Deny(format!(
                        "Missing required attribute: {}",
                        required_attr
                    )));
                }
            }
        }

        if let Some(required_level) = permission.required_level {
            if actor.clearance_level < required_level {
                return Ok(AuthDecision::Deny(format!(
                    "Insufficient clearance level: required {}, got {}",
                    required_level, actor.clearance_level
                )));
            }
        }

        Ok(AuthDecision::Allow)
    }
}

/// Composite authorization check that runs multiple checks in sequence
/// Returns Deny if any check denies, Allow only if all checks allow
pub struct CompositeAuth {
    checkers: Vec<Box<dyn AuthorizationCheck>>,
}

impl CompositeAuth {
    pub fn new() -> Self {
        Self {
            checkers: Vec::new(),
        }
    }

    pub fn add_checker(mut self, checker: Box<dyn AuthorizationCheck>) -> Self {
        self.checkers.push(checker);
        self
    }
}

impl Default for CompositeAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AuthorizationCheck for CompositeAuth {
    async fn check_permission(
        &self,
        step: &StepDefinition,
        actor: &ActorContext,
    ) -> Result<AuthDecision> {
        for checker in &self.checkers {
            match checker.check_permission(step, actor).await? {
                AuthDecision::Deny(reason) => return Ok(AuthDecision::Deny(reason)),
                AuthDecision::Allow => {}
            }
        }
        Ok(AuthDecision::Allow)
    }
}
