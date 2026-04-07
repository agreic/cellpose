<#
.SYNOPSIS
    Launches the Cellpose batch pipeline in the background.
.DESCRIPTION
    This script safely detaches the pipeline process, routes all logs to a personalized 
    D:\logs directory to prevent overwrites, and generates a tracking receipt with the PID.
.EXAMPLE
    .\Run-Cellpose.ps1 -Config "C:\Users\agreicius\Desktop\my_experiment.json"
#>

param (
    [Parameter(Mandatory=$true, HelpMessage="Please provide the path to your JSON configuration file.")]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

# 1. Validate the Config File Exists
if (!(Test-Path $Config)) {
    Write-Host "ERROR: Configuration file not found at '$Config'" -ForegroundColor Red
    Write-Host "Please check the path and try again." -ForegroundColor Yellow
    exit 1
}

$AbsoluteConfig = (Resolve-Path $Config).Path

# 2. Define the Working Directory (Forces uv to run in the correct project folder)
# $PSScriptRoot automatically resolves to D:\Tools\cellpose-main (where this script lives)
$WorkDir = $PSScriptRoot

# 3. Set Up the Unique Logging Directory
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = "D:\logs\$env:USERNAME"

if (!(Test-Path $logDir)) { 
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null 
}

$outLog = "$logDir\pipeline_${timestamp}.log"
$errLog = "$logDir\pipeline_${timestamp}_err.log"
$infoLog = "$logDir\pipeline_${timestamp}_info.txt"

# 4. Launch the Pipeline and Capture the PID
$argList = "run pipeline.py -c `"$AbsoluteConfig`""

Write-Host "Launching pipeline from: $WorkDir" -ForegroundColor Cyan

# Added -WorkingDirectory to guarantee uv finds the right python environment and pipeline.py
$process = Start-Process -FilePath "uv" `
    -ArgumentList $argList `
    -WorkingDirectory $WorkDir `
    -WindowStyle Hidden `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -PassThru

$capturedPID = $process.Id

# 5. Write the Tracking "Receipt"
$infoContent = @"
--- Cellpose Pipeline Run Tracking ---
User:        $env:USERNAME
Launched:    $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Process ID:  $capturedPID
Working Dir: $WorkDir
Config Path: $AbsoluteConfig
Command:     uv $argList

--- Log Files ---
Standard:    $outLog
Errors:      $errLog

--- Quick Commands ---
To monitor live:
Get-Content "$outLog" -Wait

To forcefully kill this exact run (if the ABORT_PIPELINE.txt trick fails):
taskkill /F /T /PID $capturedPID
"@

Set-Content -Path $infoLog -Value $infoContent

# 6. Provide Immediate User Feedback
Write-Host "Pipeline safely launched in the background." -ForegroundColor Green
Write-Host "------------------------------------------------"
Write-Host "Process ID: " -NoNewline; Write-Host "$capturedPID" -ForegroundColor Magenta
Write-Host "Receipt:    " -NoNewline; Write-Host "$infoLog" -ForegroundColor DarkGray
Write-Host "Live Log:   " -NoNewline; Write-Host "$outLog" -ForegroundColor DarkGray
Write-Host "------------------------------------------------"
Write-Host "Command to watch progress:" -ForegroundColor Yellow
Write-Host "Get-Content `"$errLog`" -Wait" -ForegroundColor Cyan