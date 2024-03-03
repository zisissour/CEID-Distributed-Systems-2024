# Array of arguments
$argumentsArray = @(1,4,9,11,14,18,20,21,28)

# Path to Python script
$nodeScriptPath = ".\node.py"

# Loop through each argument and execute the node script in a new terminal window

foreach ($arg in $argumentsArray) {
    echo "Starting node $arg"

    # Construct the command to execute the Python script with arguments
    $command = "python $nodeScriptPath $arg"

    # Start a new PowerShell process in a new terminal window
    Start-Process powershell -ArgumentList "-NoExit", "-Command $command"
}

Start-Sleep -Seconds 5

echo "Starting heartbeat and temperature generator"

$command = "python ./HBTG.py 0"

Start-Process powershell -ArgumentList "-NoExit", "-Command $command"