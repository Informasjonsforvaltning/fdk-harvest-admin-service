#!/bin/bash

# Script to trigger harvest for random datasources with name pattern "1000 $type"
# Usage: ./trigger-random-harvest.sh <org> <n> [data-type]
#   org: Organization ID (required)
#   n: Number of datasources to randomly select and trigger (required)
#   data-type: Optional data type filter (e.g., dataset, concept, etc.)

set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_SCRIPT="$SCRIPT_DIR/harvest-cli.sh"

# Default configuration
BASE_URL="${HARVEST_BASE_URL:-http://localhost:8080}"
API_KEY="${HARVEST_API_KEY:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print error message
error() {
    echo -e "${RED}Error:${NC} $1" >&2
    exit 1
}

# Print success message
success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Print warning message
warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Print info message
info() {
    echo "$1"
}

# Build curl command with common options
curl_cmd() {
    local method="$1"
    local url="$2"
    local curl_args=()
    
    if [ "$method" = "POST" ]; then
        curl_args+=("-X" "POST")
    else
        curl_args+=("-X" "GET")
    fi
    
    if [ -n "$API_KEY" ]; then
        curl_args+=("-H" "X-API-KEY: $API_KEY")
    fi
    
    curl_args+=("-s" "-w" "\n%{http_code}" "$url")
    
    # Temporarily disable unbound variable check for array expansion
    set +u
    curl "${curl_args[@]}" || error "Failed to connect to $BASE_URL"
    set -u
}

# Parse HTTP response
parse_response() {
    local response="$1"
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | sed '$d')
    
    # Check if http_code is a valid number
    if [ -z "$http_code" ] || ! [ "$http_code" -eq "$http_code" ] 2>/dev/null; then
        error "Invalid HTTP response: $response"
    fi
    
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        echo "$body"
        return 0
    else
        error "HTTP $http_code: $body"
    fi
}

# Get data sources
get_data_sources() {
    local org="$1"
    local data_type="$2"
    
    local url="$BASE_URL/internal"
    if [ -n "$org" ]; then
        url="$url/organizations/$org/datasources"
    else
        url="$url/datasources"
    fi
    
    if [ -n "$data_type" ]; then
        url="$url?dataType=$data_type"
    fi
    
    local response=$(curl_cmd "GET" "$url")
    parse_response "$response"
}

# Trigger harvest for a data source
trigger_harvest() {
    local org="$1"
    local datasource_id="$2"
    
    if [ -z "$org" ]; then
        error "Organization ID is required"
    fi
    
    if [ -z "$datasource_id" ]; then
        error "Data source ID is required"
    fi
    
    local url="$BASE_URL/organizations/$org/datasources/$datasource_id/start-harvesting"
    local response=$(curl_cmd "POST" "$url")
    local http_code=$(echo "$response" | tail -n1)
    
    if [ "$http_code" -eq 204 ]; then
        return 0
    else
        local body=$(echo "$response" | sed '$d')
        echo "Failed to trigger harvest: HTTP $http_code - $body" >&2
        return 1
    fi
}

# Check if jq is available
if ! command -v jq &> /dev/null; then
    error "jq is required for parsing JSON. Please install jq: https://stedolan.github.io/jq/download/"
fi

# Check if shuf is available (for random selection)
if ! command -v shuf &> /dev/null; then
    error "shuf is required for random selection. Please install coreutils."
fi

# Parse arguments
if [ $# -lt 2 ]; then
    error "Usage: $0 <org> <n> [data-type]

Arguments:
  org        Organization ID (required)
  n          Number of datasources to randomly select and trigger (required)
  data-type  Optional data type filter (e.g., dataset, concept, informationmodel, etc.)

Environment Variables:
  HARVEST_BASE_URL  Base URL of the harvest admin service (default: http://localhost:8080)
  HARVEST_API_KEY   API key for authentication

Example:
  $0 my-org 10 dataset
  $0 my-org 5"
fi

ORG="$1"
N="$2"
DATA_TYPE="${3:-}"

# Validate n is a positive integer
if ! [ "$N" -eq "$N" ] 2>/dev/null || [ "$N" -le 0 ]; then
    error "n must be a positive integer"
fi

info "Fetching datasources for organization: $ORG${DATA_TYPE:+ (filtered by type: $DATA_TYPE)}..."

# Get all datasources
DATA_SOURCES_JSON=$(get_data_sources "$ORG" "$DATA_TYPE")

# Filter datasources with description matching "1000 $type" pattern
# The pattern matches descriptions starting with "1000 " followed by the data type
if [ -n "$DATA_TYPE" ]; then
    # If data type is specified, match exact pattern "1000 <data-type>"
    PATTERN="^1000 $DATA_TYPE$"
    MATCHING_DATASOURCES=$(echo "$DATA_SOURCES_JSON" | jq --arg pattern "$PATTERN" '[.[] | select(.description // "" | test($pattern; "i"))]')
else
    # If no data type specified, match any "1000 *" pattern
    PATTERN="^1000 "
    MATCHING_DATASOURCES=$(echo "$DATA_SOURCES_JSON" | jq --arg pattern "$PATTERN" '[.[] | select(.description // "" | test($pattern; "i"))]')
fi

MATCHING_COUNT=$(echo "$MATCHING_DATASOURCES" | jq '. | length')

if [ "$MATCHING_COUNT" -eq 0 ]; then
    if [ -n "$DATA_TYPE" ]; then
        warning "No datasources found with description matching '1000 $DATA_TYPE' pattern"
    else
        warning "No datasources found with description matching '1000 *' pattern"
    fi
    exit 0
fi

if [ -n "$DATA_TYPE" ]; then
    info "Found $MATCHING_COUNT datasource(s) matching '1000 $DATA_TYPE' pattern"
else
    info "Found $MATCHING_COUNT datasource(s) matching '1000 *' pattern"
fi

if [ "$N" -gt "$MATCHING_COUNT" ]; then
    warning "Requested $N datasources, but only $MATCHING_COUNT are available. Using all $MATCHING_COUNT."
    N="$MATCHING_COUNT"
fi

# Randomly select n datasources
info "Randomly selecting $N datasource(s)..."
SELECTED_DATASOURCES=$(echo "$MATCHING_DATASOURCES" | jq -c '.[]' | shuf -n "$N")

if [ -z "$SELECTED_DATASOURCES" ]; then
    error "Failed to select datasources"
fi

info "Selected datasources:"
echo "$SELECTED_DATASOURCES" | jq -r '. | "  - \(.id) (\(.dataType)): \(.description // "N/A")"'

echo ""
info "Triggering harvest for selected datasources..."

SUCCESS_COUNT=0
FAILURE_COUNT=0

# Trigger harvest for each selected datasource
while IFS= read -r datasource; do
    ID=$(echo "$datasource" | jq -r '.id // empty')
    DATA_TYPE_NAME=$(echo "$datasource" | jq -r '.dataType // "unknown"')
    DESCRIPTION=$(echo "$datasource" | jq -r '.description // "N/A"')
    
    if [ -z "$ID" ] || [ "$ID" = "null" ]; then
        warning "Skipping datasource with null ID: $DESCRIPTION"
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
        continue
    fi
    
    printf "  Triggering harvest for %s datasource: %s (%s)... " "$DATA_TYPE_NAME" "$ID" "$DESCRIPTION"
    
    # Temporarily disable exit on error for this command
    set +e
    trigger_harvest "$ORG" "$ID" >/dev/null 2>&1
    RESULT=$?
    set -e
    
    if [ $RESULT -eq 0 ]; then
        echo "✓"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "✗"
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
    fi
done <<< "$SELECTED_DATASOURCES"

echo ""
info "Summary:"
info "  Successfully triggered: $SUCCESS_COUNT"
if [ "$FAILURE_COUNT" -gt 0 ]; then
    warning "  Failed: $FAILURE_COUNT"
fi
