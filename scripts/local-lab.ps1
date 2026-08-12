param(
    [ValidateSet("Start", "Stop", "Status", "Logs", "Reset")]
    [string]$Action = "Start",
    [switch]$NoBrowser,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $RepoRoot "compose.local.yml"

function Invoke-Compose {
    param([string[]]$ComposeArguments)
    & docker compose -f $ComposeFile @ComposeArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose failed with exit code $LASTEXITCODE."
    }
}

Push-Location $RepoRoot
try {
    switch ($Action) {
        "Start" {
            & docker info *> $null
            if ($LASTEXITCODE -ne 0) {
                throw "Docker Desktop is not running. Start it and run this command again."
            }
            Write-Host "Starting the persistent Kafka and SQS visual lab..." -ForegroundColor Cyan
            Invoke-Compose -ComposeArguments @("up", "-d", "--build", "--wait")
            Write-Host ""
            Write-Host "Local lab is ready." -ForegroundColor Green
            Write-Host "  Dashboard:        http://127.0.0.1:8000"
            Write-Host "  Kafka Console:    http://127.0.0.1:8088"
            Write-Host "  PostgreSQL UI:    http://127.0.0.1:8089"
            Write-Host "  API documentation:http://127.0.0.1:8000/docs"
            Write-Host ""
            Write-Host "PostgreSQL UI login: system=PostgreSQL, server=postgres, user=mqtest, password=mqtest, database=mqtest"
            if (-not $NoBrowser) {
                Start-Process "http://127.0.0.1:8000"
            }
        }
        "Stop" {
            Invoke-Compose -ComposeArguments @("stop")
            Write-Host "Local lab stopped. Topics, queues, and database rows are preserved." -ForegroundColor Green
        }
        "Status" {
            Invoke-Compose -ComposeArguments @("ps", "-a")
        }
        "Logs" {
            Invoke-Compose -ComposeArguments @("logs", "-f", "--tail", "100", "dashboard", "kafka-worker", "sqs-worker")
        }
        "Reset" {
            if (-not $Force) {
                $confirmation = Read-Host "Reset permanently deletes local Kafka, SQS, and PostgreSQL lab data. Type RESET"
                if ($confirmation -ne "RESET") {
                    Write-Host "Reset cancelled."
                    exit 0
                }
            }
            Invoke-Compose -ComposeArguments @("down", "--volumes", "--remove-orphans")
            Write-Host "Local lab containers, network, and data volumes were removed." -ForegroundColor Yellow
        }
    }
}
finally {
    Pop-Location
}
