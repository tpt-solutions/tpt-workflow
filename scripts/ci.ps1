#!/usr/bin/env pwsh
# CI script for tpt-workflow - Windows PowerShell version
# Usage: ./scripts/ci.ps1 [check|test|build|fmt|clippy|all]

param(
    [ValidateSet("check", "test", "build", "fmt", "clippy", "all")]
    [string]$Command = "all"
)

$ErrorActionPreference = "Stop"
$ROOT_DIR = Split-Path -Parent $PSScriptRoot
Set-Location $ROOT_DIR

function Run-Cargo {
    param([string]$Args)
    Write-Host "=== Running: cargo $Args ===" -ForegroundColor Cyan
    cargo $Args
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Command failed: cargo $Args"
        exit $LASTEXITCODE
    }
}

switch ($Command) {
    "check" {
        Run-Cargo "check --workspace"
    }
    "test" {
        Run-Cargo "test --workspace"
    }
    "build" {
        Run-Cargo "build --workspace --release"
    }
    "fmt" {
        Run-Cargo "fmt --all -- --check"
    }
    "clippy" {
        Run-Cargo "clippy --workspace -- -D warnings"
    }
    "all" {
        Run-Cargo "fmt --all -- --check"
        Run-Cargo "clippy --workspace -- -D warnings"
        Run-Cargo "check --workspace"
        Run-Cargo "test --workspace"
        Run-Cargo "build --workspace --release"
    }
}

Write-Host "=== CI completed successfully ===" -ForegroundColor Green