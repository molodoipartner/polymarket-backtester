@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\Start-PublicTunnel.ps1" -InstallCloudflared
pause
