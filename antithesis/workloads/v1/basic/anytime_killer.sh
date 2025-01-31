#!/busybox/sh

# Generate a random byte from /dev/urandom and extract its value (0-255)
rand_byte=$(od -An -N1 -i /dev/urandom | tr -d ' ')

# Check if the random byte falls within the 25% probability range
if [ "$rand_byte" -lt 64 ]; then
    # Get a list of PIDs of processes named "test_workload"
    pids=$(pgrep test_workload)

    # If there are any matching processes, randomly select one and kill it
    if [ -n "$pids" ]; then
        # Convert the list of PIDs into an array
        set -- $pids

        # Get the number of PIDs
        count=$#

        # Pick a random index (0-based)
        rand_index=$(od -An -N1 -i /dev/urandom | tr -d ' ')

        # Convert to a valid index (1-based for shell parameter expansion)
        rand_index=$((rand_index % count + 1))

        # Get the selected PID
        selected_pid=$(eval echo \$$rand_index)

        echo "Killing process $selected_pid"

        # Kill the selected process
        kill -9 "$selected_pid"
    fi
fi
