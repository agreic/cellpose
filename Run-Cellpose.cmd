@echo off
REM Wrapper to automatically bypass PowerShell execution policies

:: Check if the user provided a config file argument
if "%~1"=="" (
    echo [ERROR] Please provide the path to your JSON configuration file.
    echo Usage:   D:\Tools\cellpose-main\Run-Cellpose.cmd "C:\path\to\config.json"
    exit /b 1
)

:: Launch the PowerShell script with a temporary bypass
powershell.exe -ExecutionPolicy Bypass -NoProfile -File "%~dp0Run-Cellpose.ps1" -Config "%~1"