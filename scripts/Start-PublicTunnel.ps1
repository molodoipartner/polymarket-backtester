param(
    [int]$Port = 8000,
    [switch]$InstallCloudflared
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$EnvPath = Join-Path $Root ".env"
$EnvExamplePath = Join-Path $Root ".env.example"
$ToolsDir = Join-Path $Root "tools"
$LocalCloudflared = Join-Path $ToolsDir "cloudflared.exe"

function New-RandomSecret {
    param([int]$Bytes = 24)
    $buffer = New-Object byte[] $Bytes
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($buffer)
    } finally {
        $rng.Dispose()
    }
    return [Convert]::ToBase64String($buffer).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

function Get-EnvValue {
    param([string]$Key)
    if (-not (Test-Path $EnvPath)) {
        return $null
    }
    $line = Get-Content $EnvPath | Where-Object { $_ -match "^$([regex]::Escape($Key))=" } | Select-Object -First 1
    if (-not $line) {
        return $null
    }
    return $line.Substring($Key.Length + 1).Trim()
}

function Set-EnvValue {
    param([string]$Key, [string]$Value)
    $line = "$Key=$Value"
    if (-not (Test-Path $EnvPath)) {
        Set-Content -Path $EnvPath -Value $line -Encoding UTF8
        return
    }
    $content = Get-Content $EnvPath
    $found = $false
    $updated = foreach ($item in $content) {
        if ($item -match "^$([regex]::Escape($Key))=") {
            $found = $true
            $line
        } else {
            $item
        }
    }
    if (-not $found) {
        $updated += $line
    }
    Set-Content -Path $EnvPath -Value $updated -Encoding UTF8
}

function Ensure-SafePublicEnv {
    if (-not (Test-Path $EnvPath)) {
        if (Test-Path $EnvExamplePath) {
            Copy-Item $EnvExamplePath $EnvPath
        } else {
            New-Item -ItemType File -Path $EnvPath | Out-Null
        }
    }

    $sessionSecret = Get-EnvValue "SESSION_SECRET"
    if (
        [string]::IsNullOrWhiteSpace($sessionSecret) -or
        $sessionSecret.Length -lt 32 -or
        $sessionSecret -eq "change-this-local-session-secret" -or
        $sessionSecret -eq "replace-with-at-least-32-random-characters"
    ) {
        Set-EnvValue "SESSION_SECRET" (New-RandomSecret 48)
    }

    if ((Get-EnvValue "PASSWORD") -eq "TestUser123!" -or [string]::IsNullOrWhiteSpace((Get-EnvValue "PASSWORD"))) {
        Set-EnvValue "PASSWORD" ("User-" + (New-RandomSecret 12))
    }
    if ((Get-EnvValue "ADMIN_PASSWORD") -eq "Admin1234!" -or [string]::IsNullOrWhiteSpace((Get-EnvValue "ADMIN_PASSWORD"))) {
        Set-EnvValue "ADMIN_PASSWORD" ("Admin-" + (New-RandomSecret 12))
    }

    Set-EnvValue "ALLOWED_HOSTS" "127.0.0.1,localhost,*.trycloudflare.com"
    Set-EnvValue "SESSION_COOKIE_SECURE" "true"
    Set-EnvValue "ENVIRONMENT" "development"
    Set-EnvValue "HOST" "127.0.0.1"
    Set-EnvValue "PORT" "$Port"
}

Ensure-SafePublicEnv

function Find-Cloudflared {
    $command = Get-Command cloudflared -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    if (Test-Path $LocalCloudflared) {
        return $LocalCloudflared
    }
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages\Cloudflare.cloudflared_Microsoft.Winget.Source_8wekyb3d8bbwe\cloudflared.exe"),
        (Join-Path $env:ProgramFiles "cloudflared\cloudflared.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "cloudflared\cloudflared.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }
    return $null
}

function Ensure-Cloudflared {
    $path = Find-Cloudflared
    if ($path) {
        return $path
    }

    New-Item -ItemType Directory -Path $ToolsDir -Force | Out-Null
    $url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
    Write-Host "Downloading cloudflared locally..."
    try {
        Invoke-WebRequest -Uri $url -OutFile $LocalCloudflared -UseBasicParsing
    } catch {
        throw "Could not download cloudflared automatically. Check your internet connection and try again."
    }
    if (-not (Test-Path $LocalCloudflared)) {
        throw "cloudflared download failed."
    }
    return $LocalCloudflared
}

$CloudflaredPath = Ensure-Cloudflared

$python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtual environment not found at $python. Create it and install requirements first."
}

$serverScript = {
    param($Root, $Port)
    Set-Location $Root
    $env:HOST = "127.0.0.1"
    $env:PORT = "$Port"
    $env:ALLOWED_HOSTS = "127.0.0.1,localhost,*.trycloudflare.com"
    $env:SESSION_COOKIE_SECURE = "true"
    & ".venv\Scripts\python.exe" "main.py"
}

Write-Host "Starting local backend on http://127.0.0.1:$Port ..."
$server = Start-Job -ScriptBlock $serverScript -ArgumentList $Root, $Port
Start-Sleep -Seconds 3

try {
    if ($server.State -ne "Running") {
        Receive-Job $server
        throw "Local backend failed to start."
    }
    Write-Host ""
    Write-Host "Public sharing is starting." -ForegroundColor Green
    Write-Host "Copy the https://*.trycloudflare.com link printed by Cloudflare below and send it to your friend."
    Write-Host ""
    Write-Host "Login:"
    Write-Host ("  User:  " + (Get-EnvValue "USERNAME") + " / " + (Get-EnvValue "PASSWORD"))
    Write-Host ("  Admin: " + (Get-EnvValue "ADMIN_USERNAME") + " / " + (Get-EnvValue "ADMIN_PASSWORD"))
    Write-Host ""
    Write-Host "Keep this window open while your friend uses the site."
    Write-Host ""
    & $CloudflaredPath tunnel --url "http://localhost:$Port"
}
finally {
    Write-Host "Stopping local backend..."
    Stop-Job $server -ErrorAction SilentlyContinue
    Remove-Job $server -ErrorAction SilentlyContinue
}
