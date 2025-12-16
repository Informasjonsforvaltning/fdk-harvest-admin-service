#!/bin/bash

# Harvest CLI - Bash script for managing harvest operations
# Usage: ./harvest-cli.sh <command> [options]

set -euo pipefail

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

# Trigger harvest for all data sources in an organization
trigger_harvest_all() {
    local org="$1"
    
    if [ -z "$org" ]; then
        error "Organization ID is required"
    fi
    
    info "Fetching data sources for organization: $org..."
    local data_sources_json=$(get_data_sources "$org" "")
    
    # Check if jq is available for JSON parsing
    if ! command -v jq &> /dev/null; then
        error "jq is required for parsing JSON. Please install jq: https://stedolan.github.io/jq/download/"
    fi
    
    local count=$(echo "$data_sources_json" | jq '. | length')
    
    # Check if count is a valid number
    if [ -z "$count" ] || ! [ "$count" -eq "$count" ] 2>/dev/null; then
        error "Failed to parse data sources JSON"
    fi
    
    if [ "$count" -eq 0 ]; then
        warning "No data sources found for organization: $org"
        return 0
    fi
    
    info "Found $count data source(s). Triggering harvest for all..."
    
    local success_count=0
    local failure_count=0
    
    # Iterate over data sources using process substitution to avoid subshell
    while IFS= read -r datasource; do
        local id=$(echo "$datasource" | jq -r '.id // empty')
        local data_type=$(echo "$datasource" | jq -r '.dataType // "unknown"')
        
        if [ -z "$id" ] || [ "$id" = "null" ]; then
            warning "Skipping data source with null ID: $(echo "$datasource" | jq -r '.url // "unknown"')"
            failure_count=$((failure_count + 1))
            continue
        fi
        
        printf "  Triggering harvest for %s data source: %s... " "$data_type" "$id"
        
        # Temporarily disable exit on error for this command
        set +e
        trigger_harvest "$org" "$id" >/dev/null 2>&1
        local result=$?
        set -e
        
        if [ $result -eq 0 ]; then
            echo "✓"
            success_count=$((success_count + 1))
        else
            echo "✗"
            failure_count=$((failure_count + 1))
        fi
    done < <(echo "$data_sources_json" | jq -c '.[]')
    
    echo ""
    info "Summary:"
    info "  Successfully triggered: $success_count"
    if [ "$failure_count" -gt 0 ]; then
        warning "  Failed: $failure_count"
    fi
}

# List data sources
list_data_sources() {
    local org="$1"
    local data_type="$2"
    
    info "Fetching data sources..."
    local data_sources_json=$(get_data_sources "$org" "$data_type")
    
    # Check if jq is available
    if ! command -v jq &> /dev/null; then
        # Fallback: just print JSON
        echo "$data_sources_json" | jq '.'
        return 0
    fi
    
    local count=$(echo "$data_sources_json" | jq '. | length')
    
    # Check if count is a valid number
    if [ -z "$count" ] || ! [ "$count" -eq "$count" ] 2>/dev/null; then
        error "Failed to parse data sources JSON"
    fi
    
    if [ "$count" -eq 0 ]; then
        info "No data sources found."
        return 0
    fi
    
    info "Found $count data source(s):\n"
    printf "%-36s %-35s %-20s %-15s %-30s %s\n" "ID" "Description" "Data Type" "Source Type" "Publisher" "URL"
    printf "%s\n" "$(printf '%.0s-' {1..160})"
    
    echo "$data_sources_json" | jq -r '.[] | "\(.id // "N/A")\t\(.description // "N/A")\t\(.dataType)\t\(.dataSourceType)\t\(.publisherId)\t\(.url)"' | \
    while IFS=$'\t' read -r id description dtype stype publisher url; do
        if [ ${#description} -gt 33 ]; then
            description="${description:0:30}..."
        fi
        if [ -z "$description" ] || [ "$description" = "null" ]; then
            description="N/A"
        fi
        if [ ${#url} -gt 50 ]; then
            url="${url:0:47}..."
        fi
        printf "%-36s %-35s %-20s %-15s %-30s %s\n" "$id" "$description" "$dtype" "$stype" "$publisher" "$url"
    done
}

# Get harvest status
get_status() {
    local org="$1"
    local datasource_id="$2"
    local data_type="$3"
    
    if [ -z "$org" ]; then
        error "Organization ID is required"
    fi
    
    if [ -z "$datasource_id" ]; then
        error "Data source ID is required"
    fi
    
    local url="$BASE_URL/internal/organizations/$org/datasources/$datasource_id/status"
    if [ -n "$data_type" ]; then
        url="$url?dataType=$data_type"
    fi
    
    local response=$(curl_cmd "GET" "$url")
    local status_json=$(parse_response "$response")
    
    if ! command -v jq &> /dev/null; then
        echo "$status_json" | jq '.'
        return 0
    fi
    
    local count=$(echo "$status_json" | jq '. | length')
    
    # Check if count is a valid number
    if [ -z "$count" ] || ! [ "$count" -eq "$count" ] 2>/dev/null; then
        error "Failed to parse status JSON"
    fi
    
    if [ "$count" -eq 0 ]; then
        info "No harvest status found for data source: $datasource_id"
        return 0
    fi
    
    echo "$status_json" | jq -r '.[] | 
        "Data Source: \(.dataSourceId)
Data Type: \(.dataType)
Status: \(.status // "UNKNOWN")
Current Phase: \(.currentPhase // "N/A")
\(if .phaseStartedAt then "Phase Started At: \(.phaseStartedAt)" else "" end)
\(if .errorMessage then "Error: \(.errorMessage)" else "" end)
\(if .totalResources then "Total Resources: \(.totalResources)\nProcessed: \(.processedResources // 0)\nRemaining: \(.remainingResources // 0)" else "" end)
\(if .phaseEventCounts then "Phase Event Counts:
  Initiating: \(.phaseEventCounts.initiatingEventsCount // 0)
  Harvesting: \(.phaseEventCounts.harvestingEventsCount // 0)
  Reasoning: \(.phaseEventCounts.reasoningEventsCount // 0)
  RDF Parsing: \(.phaseEventCounts.rdfParsingEventsCount // 0)
  Resource Processing: \(.phaseEventCounts.resourceProcessingEventsCount // 0)
  Search Processing: \(.phaseEventCounts.searchProcessingEventsCount // 0)
  AI Search Processing: \(.phaseEventCounts.aiSearchProcessingEventsCount // 0)
  SPARQL Processing: \(.phaseEventCounts.sparqlProcessingEventsCount // 0)" else "" end)
Last Updated: \(.updatedAt)
---"'
}

# Get a specific harvest run
get_run() {
    local run_id="$1"
    
    if [ -z "$run_id" ]; then
        error "Run ID is required. Use: $0 run <run-id>"
    fi
    
    local url="$BASE_URL/internal/runs/$run_id"
    local response=$(curl_cmd "GET" "$url")
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" -eq 404 ]; then
        error "Run not found: $run_id"
    elif [ "$http_code" -ne 200 ]; then
        error "Failed to get run: HTTP $http_code - $body"
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "$body" | jq '.'
        return 0
    fi
    
    echo "$body" | jq -r '
        "Run ID: \(.runId)
Data Source ID: \(.dataSourceId)
Data Type: \(.dataType)
Status: \(.status)
Started At: \(.runStartedAt // "N/A")
Ended At: \(.runEndedAt // "N/A")
Total Duration: \(if .totalDurationMs then (.totalDurationMs / 1000 | tostring + "s") else "N/A" end)

Phase Durations:
  Init: \(if .phaseDurations.initDurationMs then (.phaseDurations.initDurationMs / 1000 | tostring + "s") else "N/A" end)
  Harvest: \(if .phaseDurations.harvestDurationMs then (.phaseDurations.harvestDurationMs / 1000 | tostring + "s") else "N/A" end)
  Reasoning: \(if .phaseDurations.reasoningDurationMs then (.phaseDurations.reasoningDurationMs / 1000 | tostring + "s") else "N/A" end)
  RDF Parsing: \(if .phaseDurations.rdfParsingDurationMs then (.phaseDurations.rdfParsingDurationMs / 1000 | tostring + "s") else "N/A" end)
  Search Processing: \(if .phaseDurations.searchProcessingDurationMs then (.phaseDurations.searchProcessingDurationMs / 1000 | tostring + "s") else "N/A" end)
  AI Search Processing: \(if .phaseDurations.aiSearchProcessingDurationMs then (.phaseDurations.aiSearchProcessingDurationMs / 1000 | tostring + "s") else "N/A" end)
  Resource Processing: \(if .phaseDurations.apiProcessingDurationMs then (.phaseDurations.apiProcessingDurationMs / 1000 | tostring + "s") else "N/A" end)
  SPARQL Processing: \(if .phaseDurations.sparqlProcessingDurationMs then (.phaseDurations.sparqlProcessingDurationMs / 1000 | tostring + "s") else "N/A" end)

Resource Counts:
  Total: \(.resourceCounts.totalResources // "N/A")
  Changed: \(.resourceCounts.changedResourcesCount // "N/A")
  Unchanged: \(.resourceCounts.unchangedResourcesCount // "N/A")
  Removed: \(.resourceCounts.removedResourcesCount // "N/A")
\(if .resourceCounts.phaseEventCounts then "Phase Event Counts:
  Initiating: \(.resourceCounts.phaseEventCounts.initiatingEventsCount // "N/A")
  Harvesting: \(.resourceCounts.phaseEventCounts.harvestingEventsCount // "N/A")
  Reasoning: \(.resourceCounts.phaseEventCounts.reasoningEventsCount // "N/A")
  RDF Parsing: \(.resourceCounts.phaseEventCounts.rdfParsingEventsCount // "N/A")
  Resource Processing: \(.resourceCounts.phaseEventCounts.resourceProcessingEventsCount // "N/A")
  Search Processing: \(.resourceCounts.phaseEventCounts.searchProcessingEventsCount // "N/A")
  AI Search Processing: \(.resourceCounts.phaseEventCounts.aiSearchProcessingEventsCount // "N/A")
  SPARQL Processing: \(.resourceCounts.phaseEventCounts.sparqlProcessingEventsCount // "N/A")" else "" end)
\(if .errorMessage then "Error: \(.errorMessage)" else "" end)
Created: \(.createdAt)
Updated: \(.updatedAt)"'
}

# List harvest runs
list_runs() {
    local data_source_id="$1"
    local data_type="$2"
    local status="$3"
    local offset="${4:-0}"
    local limit="${5:-50}"
    
    local url="$BASE_URL/internal/runs"
    local params=()
    
    [ -n "$data_source_id" ] && params+=("dataSourceId=$data_source_id")
    [ -n "$data_type" ] && params+=("dataType=$data_type")
    [ -n "$status" ] && params+=("status=$status")
    [ -n "$offset" ] && params+=("offset=$offset")
    [ -n "$limit" ] && params+=("limit=$limit")
    
    if [ ${#params[@]} -gt 0 ]; then
        url="$url?$(IFS='&'; echo "${params[*]}")"
    fi
    
    local response=$(curl_cmd "GET" "$url")
    local runs_json=$(parse_response "$response")
    
    if ! command -v jq &> /dev/null; then
        echo "$runs_json" | jq '.'
        return 0
    fi
    
    # Check if response is an object with runs/total or just an array
    local response_type=$(echo "$runs_json" | jq 'type')
    
    if [ "$response_type" = '"object"' ]; then
        # Response is an object with runs and total
        local total=$(echo "$runs_json" | jq -r '.total // 0')
        local runs=$(echo "$runs_json" | jq '.runs // []')
        local count=$(echo "$runs" | jq '. | length')
        
        # Check if count is a valid number
        if [ -z "$count" ] || ! [ "$count" -eq "$count" ] 2>/dev/null; then
            error "Failed to parse runs JSON"
        fi
        
        if [ "$count" -eq 0 ]; then
            info "No harvest runs found."
            return 0
        fi
        
        info "Found $count run(s) (total: $total):\n"
    else
        # Response is directly an array
        local runs="$runs_json"
        local count=$(echo "$runs" | jq '. | length')
        local total="$count"
        
        # Check if count is a valid number
        if [ -z "$count" ] || ! [ "$count" -eq "$count" ] 2>/dev/null; then
            error "Failed to parse runs JSON"
        fi
        
        if [ "$count" -eq 0 ]; then
            info "No harvest runs found."
            return 0
        fi
        
        info "Found $count run(s):\n"
    fi
    
    printf "%-36s %-36s %-15s %-12s %-20s %s\n" "Run ID" "Data Source ID" "Data Type" "Status" "Started At" "Duration"
    printf "%s\n" "$(printf '%.0s-' {1..140})"
    
    echo "$runs" | jq -r '.[] | "\(.runId)\t\(.dataSourceId)\t\(.dataType)\t\(.status)\t\(.runStartedAt // "N/A")\t\(if .totalDurationMs then (.totalDurationMs / 1000 | tostring + "s") else "N/A" end)"' | \
    while IFS=$'\t' read -r run_id ds_id dtype status started duration; do
        if [ ${#ds_id} -gt 35 ]; then
            ds_id="${ds_id:0:32}..."
        fi
        printf "%-36s %-36s %-15s %-12s %-20s %s\n" "$run_id" "$ds_id" "$dtype" "$status" "$started" "$duration"
    done
}

# Get performance metrics
get_metrics() {
    local data_source_id="$1"
    local data_type="$2"
    local days_back="$3"
    local start_date="$4"
    local end_date="$5"
    local limit="$6"
    
    local url="$BASE_URL/internal/runs/metrics"
    local params=()
    
    [ -n "$data_source_id" ] && params+=("dataSourceId=$data_source_id")
    [ -n "$data_type" ] && params+=("dataType=$data_type")
    [ -n "$days_back" ] && params+=("daysBack=$days_back")
    [ -n "$start_date" ] && params+=("startDate=$start_date")
    [ -n "$end_date" ] && params+=("endDate=$end_date")
    [ -n "$limit" ] && params+=("limit=$limit")
    
    if [ ${#params[@]} -gt 0 ]; then
        url="$url?$(IFS='&'; echo "${params[*]}")"
    fi
    
    local response=$(curl_cmd "GET" "$url")
    local http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" -eq 404 ]; then
        error "No metrics found"
    elif [ "$http_code" -ne 200 ]; then
        error "Failed to get metrics: HTTP $http_code - $body"
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "$body" | jq '.'
        return 0
    fi
    
    echo "$body" | jq -r '
        "Performance Metrics
\(if .dataSourceId then "Data Source ID: \(.dataSourceId)" else "Global Metrics" end)
\(if .dataType then "Data Type: \(.dataType)" else "" end)
Period: \(.periodStart // "N/A") to \(.periodEnd // "N/A")

Run Statistics:
  Total Runs: \(.totalRuns)
  Completed: \(.completedRuns)
  Failed: \(.failedRuns)

Average Durations (seconds):
  Total: \(if .averageTotalDurationMs then (.averageTotalDurationMs / 1000 | tostring) else "N/A" end)
  Harvest: \(if .averageHarvestDurationMs then (.averageHarvestDurationMs / 1000 | tostring) else "N/A" end)
  Reasoning: \(if .averageReasoningDurationMs then (.averageReasoningDurationMs / 1000 | tostring) else "N/A" end)
  RDF Parsing: \(if .averageRdfParsingDurationMs then (.averageRdfParsingDurationMs / 1000 | tostring) else "N/A" end)
  Search Processing: \(if .averageSearchProcessingDurationMs then (.averageSearchProcessingDurationMs / 1000 | tostring) else "N/A" end)
  AI Search Processing: \(if .averageAiSearchProcessingDurationMs then (.averageAiSearchProcessingDurationMs / 1000 | tostring) else "N/A" end)
  Resource Processing: \(if .averageApiProcessingDurationMs then (.averageApiProcessingDurationMs / 1000 | tostring) else "N/A" end)
  SPARQL Processing: \(if .averageSparqlProcessingDurationMs then (.averageSparqlProcessingDurationMs / 1000 | tostring) else "N/A" end)"'
}

# Show usage
show_usage() {
    cat << EOF
Harvest CLI - Bash script for managing harvest operations

Usage: $0 <command> [options]

Commands:
  trigger <org> [datasource-id]    Trigger harvest for a data source or all data sources in an organization
  list [org] [--data-type TYPE]     List data sources
  status <org> <datasource-id>      Get harvest status for a data source
  run <run-id>                      Get detailed information about a specific harvest run
  runs [options]                    List harvest runs with optional filters
  metrics [options]                 Get performance metrics
  help                              Show this help message

Environment Variables:
  HARVEST_BASE_URL                  Base URL of the harvest admin service (default: http://localhost:8080)
  HARVEST_API_KEY                   API key for authentication

Examples:
  # Trigger harvest for all data sources in an organization
  $0 trigger example-org

  # Trigger harvest for a specific data source
  $0 trigger example-org abc-123-def-456

  # List all data sources
  $0 list

  # List data sources for an organization
  $0 list example-org

  # List data sources filtered by type
  $0 list example-org --data-type dataset

  # Get harvest status
  $0 status example-org abc-123-def-456

  # Get a specific harvest run
  $0 run abc-123-def-456-789

  # List harvest runs
  $0 runs
  $0 runs --data-source-id abc-123 --data-type dataset --status COMPLETED
  $0 runs --offset 0 --limit 10

  # Get performance metrics
  $0 metrics
  $0 metrics --data-source-id abc-123 --data-type dataset
  $0 metrics --days-back 7
  $0 metrics --start-date 2024-01-01T00:00:00Z --end-date 2024-01-31T23:59:59Z
  $0 metrics --limit 10

  # Using environment variables
  export HARVEST_BASE_URL=https://harvest.example.com
  export HARVEST_API_KEY=my-api-key
  $0 trigger example-org
EOF
}

# Main script logic
main() {
    local command="${1:-help}"
    
    case "$command" in
        trigger)
            local org="${2:-}"
            local datasource_id="${3:-}"
            
            if [ -z "$org" ]; then
                error "Organization ID is required. Use: $0 trigger <org> [datasource-id]"
            fi
            
            if [ -n "$datasource_id" ]; then
                if trigger_harvest "$org" "$datasource_id"; then
                    success "Harvest triggered successfully for data source: $datasource_id"
                else
                    error "Failed to trigger harvest"
                fi
            else
                trigger_harvest_all "$org"
            fi
            ;;
        list)
            local org=""
            local data_type=""
            
            # Parse arguments
            shift  # Remove 'list' command
            while [ $# -gt 0 ]; do
                case "$1" in
                    --data-type)
                        if [ $# -lt 2 ]; then
                            error "--data-type requires a value"
                        fi
                        data_type="$2"
                        shift 2
                        ;;
                    *)
                        if [ -z "$org" ]; then
                            org="$1"
                        else
                            error "Unexpected argument: $1"
                        fi
                        shift
                        ;;
                esac
            done
            
            list_data_sources "$org" "$data_type"
            ;;
        status)
            local org="${2:-}"
            local datasource_id="${3:-}"
            local data_type="${4:-}"
            
            if [ -z "$org" ] || [ -z "$datasource_id" ]; then
                error "Organization ID and data source ID are required. Use: $0 status <org> <datasource-id> [data-type]"
            fi
            
            get_status "$org" "$datasource_id" "$data_type"
            ;;
        run)
            local run_id="${2:-}"
            get_run "$run_id"
            ;;
        runs)
            local data_source_id=""
            local data_type=""
            local status=""
            local offset="0"
            local limit="50"
            
            shift  # Remove 'runs' command
            while [ $# -gt 0 ]; do
                case "$1" in
                    --data-source-id|--datasource-id)
                        if [ $# -lt 2 ]; then
                            error "--data-source-id requires a value"
                        fi
                        data_source_id="$2"
                        shift 2
                        ;;
                    --data-type)
                        if [ $# -lt 2 ]; then
                            error "--data-type requires a value"
                        fi
                        data_type="$2"
                        shift 2
                        ;;
                    --status)
                        if [ $# -lt 2 ]; then
                            error "--status requires a value"
                        fi
                        status="$2"
                        shift 2
                        ;;
                    --offset)
                        if [ $# -lt 2 ]; then
                            error "--offset requires a value"
                        fi
                        offset="$2"
                        shift 2
                        ;;
                    --limit)
                        if [ $# -lt 2 ]; then
                            error "--limit requires a value"
                        fi
                        limit="$2"
                        shift 2
                        ;;
                    *)
                        error "Unexpected argument: $1"
                        ;;
                esac
            done
            
            list_runs "$data_source_id" "$data_type" "$status" "$offset" "$limit"
            ;;
        metrics)
            local data_source_id=""
            local data_type=""
            local days_back=""
            local start_date=""
            local end_date=""
            local limit=""
            
            shift  # Remove 'metrics' command
            while [ $# -gt 0 ]; do
                case "$1" in
                    --data-source-id|--datasource-id)
                        if [ $# -lt 2 ]; then
                            error "--data-source-id requires a value"
                        fi
                        data_source_id="$2"
                        shift 2
                        ;;
                    --data-type)
                        if [ $# -lt 2 ]; then
                            error "--data-type requires a value"
                        fi
                        data_type="$2"
                        shift 2
                        ;;
                    --days-back)
                        if [ $# -lt 2 ]; then
                            error "--days-back requires a value"
                        fi
                        days_back="$2"
                        shift 2
                        ;;
                    --start-date)
                        if [ $# -lt 2 ]; then
                            error "--start-date requires a value"
                        fi
                        start_date="$2"
                        shift 2
                        ;;
                    --end-date)
                        if [ $# -lt 2 ]; then
                            error "--end-date requires a value"
                        fi
                        end_date="$2"
                        shift 2
                        ;;
                    --limit)
                        if [ $# -lt 2 ]; then
                            error "--limit requires a value"
                        fi
                        limit="$2"
                        shift 2
                        ;;
                    *)
                        error "Unexpected argument: $1"
                        ;;
                esac
            done
            
            get_metrics "$data_source_id" "$data_type" "$days_back" "$start_date" "$end_date" "$limit"
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            error "Unknown command: $command\n\n$(show_usage)"
            ;;
    esac
}

# Run main function
main "$@"
