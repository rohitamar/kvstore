$logFile = ".\execution_log"
Clear-Content $logFile

$commandString = "make del && make war && war"
$iterations = 5

Write-Host "Running $iterations iterations:" -ForegroundColor Cyan

for ($i = 1; $i -le $iterations; $i++) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $header = "`n================ Iteration $i of $iterations [$timestamp] ================"
    
    Write-Host "Running iteration $i." -NoNewline
    
    Add-Content -Path $logFile -Value $header
    
    cmd /c $commandString 2>&1 | Out-File -FilePath $logFile -Append -Encoding UTF8
    
    Write-Host " Done." -ForegroundColor Green
}