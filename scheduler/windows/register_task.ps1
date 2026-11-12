# Registers a Windows Task Scheduler task that runs the pipeline every 24 hours.
#
#   powershell -ExecutionPolicy Bypass -File scheduler\windows\register_task.ps1
#   powershell -ExecutionPolicy Bypass -File scheduler\windows\register_task.ps1 -Time "03:30"
#
# The task runs as the current user (no admin rights needed). By default it
# only runs while you are logged on; to run it when logged off, open Task
# Scheduler > BankDatabasePipeline > Properties and choose
# "Run whether user is logged on or not".

param(
    [string]$Time = "02:00",
    [string]$TaskName = "BankDatabasePipeline"
)

$Wrapper = Join-Path $PSScriptRoot "run_pipeline.ps1"
$ProjectDir = Resolve-Path (Join-Path $PSScriptRoot "..\..")

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Wrapper`"" `
    -WorkingDirectory $ProjectDir

$trigger = New-ScheduledTaskTrigger -Daily -At $Time

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 30)
# MultipleInstances IgnoreNew  - never start a second copy while one is running
# StartWhenAvailable           - if the PC was off at $Time, run as soon as it is on again
# RestartCount/RestartInterval - retry a failed run twice, 30 minutes apart

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "bank-database-pipeline: daily ETL run (logs in $ProjectDir\logs)" -Force | Out-Null

Write-Host "Task '$TaskName' registered: every day at $Time"
Write-Host "Run it now:   Start-ScheduledTask -TaskName $TaskName"
Write-Host "Last result:  Get-ScheduledTaskInfo -TaskName $TaskName"
