$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
$composeFile = Join-Path $rootDir "docker-compose.yml"
$dataDir = Join-Path $rootDir "data_repository"

function Compose-Down {
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        docker-compose -f $composeFile down -v
        return
    }
    docker compose -f $composeFile down -v
}

function Compose-Up {
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        docker-compose -f $composeFile up -d
        return
    }
    docker compose -f $composeFile up -d
}

function Wait-ForUrl {
    param(
        [string]$Name,
        [string]$Url
    )

    $attempts = 30
    $waitSeconds = 2

    for ($i = 1; $i -le $attempts; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -Method Get -TimeoutSec 5
            if ($resp.StatusCode -eq 200) {
                Write-Host "Ready: $Name"
                return
            }
        } catch {
        }
        Start-Sleep -Seconds $waitSeconds
    }

    throw "Timeout waiting for $Name at $Url"
}

Write-Host "Stopping cluster and removing volumes..."
Compose-Down

Write-Host "Clearing data_repository contents..."
if (Test-Path $dataDir) {
    Get-ChildItem -Force -Path $dataDir | Where-Object {
        $_.Name -ne ".gitignore" -and $_.Name -ne ".gitkeep"
    } | Remove-Item -Recurse -Force
}

Write-Host "Starting cluster..."
Compose-Up

Write-Host "Waiting for services..."
Wait-ForUrl -Name "ingestion1" -Url "http://localhost:7001/ingest/list"
Wait-ForUrl -Name "ingestion2" -Url "http://localhost:7002/ingest/list"
Wait-ForUrl -Name "ingestion3" -Url "http://localhost:7003/ingest/list"
Wait-ForUrl -Name "indexing1" -Url "http://localhost:7101/health"
Wait-ForUrl -Name "indexing2" -Url "http://localhost:7102/health"
Wait-ForUrl -Name "indexing3" -Url "http://localhost:7103/health"
Wait-ForUrl -Name "search1" -Url "http://localhost:8000/status"
Wait-ForUrl -Name "search2" -Url "http://localhost:8001/status"
Wait-ForUrl -Name "search3" -Url "http://localhost:8002/status"

Write-Host "Reset complete."
