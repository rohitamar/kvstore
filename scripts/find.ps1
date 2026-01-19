$filePath = "C:\Users\rohit\Coding\kvstore\newdatafiles\1768770887087645400"

$content = [System.IO.File]::ReadAllBytes($filePath)

$enc = [System.Text.Encoding]::GetEncoding("ISO-8859-1")
$text = $enc.GetString($content)

$searchString = "TUKUCIRFTM"
$index = 0
$positions = @()

while ($index -lt $text.Length) {
    $index = $text.IndexOf($searchString, $index)
    
    if ($index -eq -1) {
        break
    }
    
    $positions += $index
    Write-Host "Found at BYTE position: $index"
    
    $index += $searchString.Length
}

Write-Host "Total occurrences: $($positions.Count)"