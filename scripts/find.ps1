$folderPath = "C:\Users\rohit\Coding\kvstore\datafiles" 

$searchString = "HKUQAPAQZR"
$enc = [System.Text.Encoding]::GetEncoding("ISO-8859-1")
$globalTotal = 0

$files = Get-ChildItem -Path $folderPath -File

Write-Host "Scanning $($files.Count) files in $folderPath..." -ForegroundColor Yellow
Write-Host "------------------------------------------------"

foreach ($file in $files) {
    
    # Read the specific file
    $content = [System.IO.File]::ReadAllBytes($file.FullName)
    $text = $enc.GetString($content)

    $index = 0
    $filePositions = @()

    while ($index -lt $text.Length) {
        $index = $text.IndexOf($searchString, $index)
        
        if ($index -eq -1) {
            break
        }
        
        $filePositions += $index
        $index += $searchString.Length
    }

    if ($filePositions.Count -gt 0) {
        Write-Host "File: $($file.Name)" -ForegroundColor Cyan
        foreach ($pos in $filePositions) {
            Write-Host "   Found at BYTE position: $pos"
        }
        $globalTotal += $filePositions.Count
    }
}

Write-Host "------------------------------------------------"
Write-Host "Total occurrences across all files: $globalTotal" -ForegroundColor Green