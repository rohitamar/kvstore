$filePath = "C:\Users\rohit\Coding\kvstore\newdatafiles\1768770175021078100"
$byteOffset = 2936
$numBytes = 5

# Open the file safely in Read mode
$stream = [System.IO.File]::OpenRead($filePath)

if ($byteOffset + $numBytes -le $stream.Length) {
    $stream.Seek($byteOffset, [System.IO.SeekOrigin]::Begin) | Out-Null
    $buffer = New-Object byte[] $numBytes
    $stream.Read($buffer, 0, $numBytes) | Out-Null

    Write-Host "--- Raw Data at Offset $byteOffset ---"
    
    $hexString = ($buffer | ForEach-Object { $_.ToString("X2") }) -join " "
    Write-Host "HEX Values: $hexString"

    $textString = [System.Text.Encoding]::ASCII.GetString($buffer)
    
    # replace unprintable characters with a dot
    $safeText = $textString -replace '[^a-zA-Z0-9]', '.'
    Write-Host "Text View: $safeText"

} else {
    Write-Host "Error: Offset is beyond the file size."
}

# Always close the stream to release the file lock
$stream.Close()