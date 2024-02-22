#!/bin/bash

# Function to perform nslookup checks
perform_nslookup_check() {
    local domain=$1

    local result=$(nslookup "$domain" 2>&1 | tr '\n' ' ')

    if [[ "$result" =~ "can't find" ]]; then
        # Return the error message
        echo "Failed nslookup for $domain: $result"
    fi
}

perform_curl_check() {
    # Execute the curl command, capture the status code
    local url=$1
    local status_code=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$url")

    # Check if the HTTP status code is not 2xx (successful) or 302 (redirect)
    if [[ ! $status_code =~ ^2 ]] && [[ $status_code -ne 302 ]]; then
        # curl request was not successful, return the error message with the status code
        echo "Failed request to $url with HTTP status code: $status_code"
    fi
}

# Function to check MQTT connection
check_tcp_connection() {
    local host=$1
    local port=$2

    # Check if a connection can be made to the MQTT port
    if ! nc -z -w5 "$host" "$port" >/dev/null 2>&1; then
        # Return the error message
        echo "Failed to connect to host $host on TCP port $port"
    fi
}
check_udp_connection() {
    local host=$1
    local port=$2

    # Send an empty string to the UDP port and check if the connection can be made
    if ! echo -n | nc -u -z -w5 "$host" "$port" >/dev/null 2>&1; then
        # Return the error message
        echo "Failed to connect to host $host on UDP port $port"
    fi
}

# Array of checks, each element is a command to execute
declare -a checks=(
    "perform_nslookup_check api.lumeo.com"
    "perform_nslookup_check mqtt.lumeo.com"
    "check_tcp_connection mqtt.lumeo.com 8883"
    "perform_curl_check https://api.lumeo.com/v1/internal/version"
    "perform_curl_check https://assets.lumeo.com/wgm/lumeo-wgm-update.sh"    
    "perform_curl_check https://lumeo-releases.ams3.digitaloceanspaces.com/lumeod/lumeo-container-update.sh"
    "perform_curl_check https://lumeo-releases.ams3.cdn.digitaloceanspaces.com/lumeod/lumeo-container-update.sh"
    "perform_curl_check https://lumeo-api.sfo2.digitaloceanspaces.com/connectivity_check/check.txt"
    "perform_curl_check https://lumeo-api.sfo2.cdn.digitaloceanspaces.com/connectivity_check/check.txt"
    "perform_curl_check https://storage.cloud.google.com"
    "perform_curl_check https://storage.googleapis.com/lumeo-public/check.txt"
    "perform_nslookup_check stun.l.google.com"
    "perform_nslookup_check traverse.lumeo.com"
    "perform_nslookup_check media.lumeo.com"
    "check_udp_connection traverse.lumeo.com 3478"
    "check_tcp_connection traverse.lumeo.com 3478"
    "check_udp_connection stun.l.google.com 3478"
    "check_tcp_connection media.lumeo.com 322"
)


# Iterate through checks and collect error messages
error_message=""
for check_command in "${checks[@]}"; do
    error_result=$($check_command)
    if [ -n "$error_result" ]; then
        error_message+="$error_result; "
    fi
done

# Output results
if [ -z "$error_message" ]; then
    echo '{"status": "success","message": "All checks passed successfully"}'
else
    # Create a JSON object with the error message
    jq -n --arg message "$error_message" '{"status": "error", "message": $message}'
fi
