# Removes the scheduled task created by register_task.ps1.
param([string]$TaskName = "BankDatabasePipeline")

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "Task '$TaskName' removed"
