# Wrapper started by Windows Task Scheduler.
# Runs the pipeline with the project's virtual environment and writes one
# line per run (start, end, exit code) to logs\scheduler.log.

$ProjectDir = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $ProjectDir

$Python = Join-Path $ProjectDir ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    # no virtual environment: use the "py" launcher, or "python" from PATH
    $Python = if (Get-Command py -ErrorAction SilentlyContinue) { "py" } else { "python" }
}

$LogDir = Join-Path $ProjectDir "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$SchedulerLog = Join-Path $LogDir "scheduler.log"

$start = Get-Date
Add-Content $SchedulerLog "$($start.ToString('yyyy-MM-dd HH:mm:ss')) START"

# The pipeline reads its settings from .env itself.
# Console output is discarded here - everything is already in logs\pipeline.log.
& $Python "scripts\run_pipeline.py" *> $null
$code = $LASTEXITCODE

$status = switch ($code) { 0 { "SUCCESS" } 3 { "SKIPPED (already running)" } default { "FAILED" } }
$seconds = [int]((Get-Date) - $start).TotalSeconds
Add-Content $SchedulerLog "$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')) END   exit=$code $status (${seconds}s)"

exit $code
