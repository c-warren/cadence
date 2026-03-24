#!/bin/bash

# Query Cassandra DLQ tables from dev-cassandra-1 container
# Tables: history_task_dlq_point and history_task_dlq_range

set -e

CONTAINER="dev-cassandra-1"
KEYSPACE="cadence_cluster1"
LIMIT="${LIMIT:-100}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

function run_cql() {
    docker exec "$CONTAINER" cqlsh -e "$1"
}

function show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -c, --count             Show count of records in each table"
    echo "  --tasks-only            Count only task rows (exclude ack level rows for range table)"
    echo "  -p, --point             Query history_task_dlq_point table only"
    echo "  -r, --range             Query history_task_dlq_range table only"
    echo "  -simp, --simplified     Query history_task_dlq table only"
    echo "  -s, --shard SHARD_ID    Filter by shard_id"
    echo "  -d, --domain DOMAIN_ID  Filter by domain_id"
    echo "  -t, --task-type TYPE    Filter by task_type"
    echo "  -l, --limit LIMIT       Limit number of results (default: 100)"
    echo "  -a, --all               Query all tables (default)"
    echo "  -k, --keyspace KEYSPACE Filter by cassandra keyspace (default: cadence_cluster1)"
    echo ""
    echo "Environment variables:"
    echo "  LIMIT                   Default limit for queries (default: 100)"
    echo ""
    echo "Examples:"
    echo "  $0                      # Query both tables with default limit"
    echo "  $0 --count              # Show counts only"
    echo "  $0 --count --tasks-only # Count only task rows (exclude ack levels)"
    echo "  $0 --point --limit 50   # Query point table with limit 50"
    echo "  $0 --shard 1            # Filter by shard_id = 1"
}

# Parse command line arguments
COUNT_ONLY=false
TASKS_ONLY=false
POINT_ONLY=false
RANGE_ONLY=false
SIMPLIFIED_ONLY=false
SHARD_FILTER=""
DOMAIN_FILTER=""
TASK_TYPE_FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -c|--count)
            COUNT_ONLY=true
            shift
            ;;
        --tasks-only)
            TASKS_ONLY=true
            shift
            ;;
        -p|--point)
            POINT_ONLY=true
            shift
            ;;
        -r|--range)
            RANGE_ONLY=true
            shift
            ;;
        -simp|--simplified)
            SIMPLIFIED_ONLY=true
            shift
            ;;
        -s|--shard)
            SHARD_FILTER="$2"
            shift 2
            ;;
        -d|--domain)
            DOMAIN_FILTER="$2"
            shift 2
            ;;
        -t|--task-type)
            TASK_TYPE_FILTER="$2"
            shift 2
            ;;
        -l|--limit)
            LIMIT="$2"
            shift 2
            ;;
        -k|--keyspace)
            KEYSPACE="$2"
            shift 2
            ;;
        -a|--all)
            POINT_ONLY=false
            RANGE_ONLY=false
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_usage
            exit 1
            ;;
    esac
done

# If neither point nor range specified, query both
if [ "$POINT_ONLY" = false ] && [ "$RANGE_ONLY" = false ] && [ "$SIMPLIFIED_ONLY" = false ]; then
    POINT_ONLY=true
    RANGE_ONLY=true
    SIMPLIFIED_ONLY=true
fi

# Build WHERE clause for filters
WHERE_CLAUSE=""
if [ -n "$SHARD_FILTER" ]; then
    WHERE_CLAUSE="WHERE shard_id = $SHARD_FILTER"
fi

print_header "Querying Cassandra DLQ Tables in $KEYSPACE"

# Count queries
if [ "$COUNT_ONLY" = true ]; then
    if [ "$POINT_ONLY" = true ]; then
        print_header "Count: history_task_dlq_point"
        QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq_point ${WHERE_CLAUSE};"
        echo -e "${YELLOW}Query:${NC} $QUERY\n"
        run_cql "$QUERY"
    fi

    if [ "$RANGE_ONLY" = true ]; then
        if [ "$TASKS_ONLY" = true ]; then
            print_header "Count: history_task_dlq_range (task rows only, excluding ack levels)"
            # Add row_type filter to WHERE clause
            if [ -n "$WHERE_CLAUSE" ]; then
                RANGE_WHERE="${WHERE_CLAUSE} AND row_type = 0"
            else
                RANGE_WHERE="WHERE row_type = 0"
            fi
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq_range ${RANGE_WHERE} ALLOW FILTERING;"
        else
            print_header "Count: history_task_dlq_range (all rows)"
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq_range ${WHERE_CLAUSE};"
        fi
        echo -e "${YELLOW}Query:${NC} $QUERY\n"
        run_cql "$QUERY"

        # If tasks-only, also show ack level count separately
        if [ "$TASKS_ONLY" = true ]; then
            print_header "Count: history_task_dlq_range (ack level rows only)"
            if [ -n "$WHERE_CLAUSE" ]; then
                ACK_WHERE="${WHERE_CLAUSE} AND row_type = 1"
            else
                ACK_WHERE="WHERE row_type = 1"
            fi
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq_range ${ACK_WHERE} ALLOW FILTERING;"
            echo -e "${YELLOW}Query:${NC} $QUERY\n"
            run_cql "$QUERY"
        fi
    fi

    if [ "$SIMPLIFIED_ONLY" = true ]; then
        if [ "$TASKS_ONLY" = true ]; then
            print_header "Count: history_task_dlq (task rows only, excluding ack levels)"
            # Add task_id filter to WHERE clause (task_id != -1 for task rows)
            if [ -n "$WHERE_CLAUSE" ]; then
                SIMPLIFIED_WHERE="${WHERE_CLAUSE} AND task_id != -1"
            else
                SIMPLIFIED_WHERE="WHERE task_id != -1"
            fi
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq ${SIMPLIFIED_WHERE} ALLOW FILTERING;"
        else
            print_header "Count: history_task_dlq (all rows)"
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq ${WHERE_CLAUSE};"
        fi
        echo -e "${YELLOW}Query:${NC} $QUERY\n"
        run_cql "$QUERY"

        # If tasks-only, also show ack level count separately
        if [ "$TASKS_ONLY" = true ]; then
            print_header "Count: history_task_dlq (ack level rows only)"
            if [ -n "$WHERE_CLAUSE" ]; then
                ACK_WHERE="${WHERE_CLAUSE} AND task_id = -1"
            else
                ACK_WHERE="WHERE task_id = -1"
            fi
            QUERY="SELECT COUNT(*) FROM ${KEYSPACE}.history_task_dlq ${ACK_WHERE} ALLOW FILTERING;"
            echo -e "${YELLOW}Query:${NC} $QUERY\n"
            run_cql "$QUERY"
        fi
    fi

    exit 0
fi

# Query history_task_dlq_point
if [ "$POINT_ONLY" = true ]; then
    print_header "Query: history_task_dlq_point"

    QUERY="SELECT shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, visibility_timestamp, task_id, workflow_id, run_id, encoding_type, version, created_at, updated_at FROM ${KEYSPACE}.history_task_dlq_point ${WHERE_CLAUSE} LIMIT ${LIMIT};"

    echo -e "${YELLOW}Query:${NC} $QUERY\n"
    run_cql "EXPAND ON; $QUERY"
fi

# Query history_task_dlq_range
if [ "$RANGE_ONLY" = true ]; then
    print_header "Query: history_task_dlq_range"

    QUERY="SELECT shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, row_type, visibility_timestamp, task_id, workflow_id, run_id, encoding_type, version, created_at, updated_at FROM ${KEYSPACE}.history_task_dlq_range ${WHERE_CLAUSE} LIMIT ${LIMIT};"

    echo -e "${YELLOW}Query:${NC} $QUERY\n"
    run_cql "EXPAND ON; $QUERY"
fi
#
# Query history_task_dlq_range
if [ "$SIMPLIFIED_ONLY" = true ]; then
    print_header "Query: history_task_dlq"

    QUERY="SELECT shard_id, domain_id, cluster_attribute_scope, cluster_attribute_name, task_type, task_id, visibility_timestamp, workflow_id, run_id, encoding_type, version, ack_level_value, created_at FROM ${KEYSPACE}.history_task_dlq ${WHERE_CLAUSE} LIMIT ${LIMIT};"

    echo -e "${YELLOW}Query:${NC} $QUERY\n"
    run_cql "EXPAND ON; $QUERY"
fi

# Show summary
print_header "Summary"
echo -e "${GREEN}Query completed successfully${NC}"
echo -e "Container: ${BLUE}$CONTAINER${NC}"
echo -e "Keyspace: ${BLUE}$KEYSPACE${NC}"
echo -e "Limit: ${BLUE}$LIMIT${NC}"
if [ -n "$WHERE_CLAUSE" ]; then
    echo -e "Filters: ${BLUE}$WHERE_CLAUSE${NC}"
fi
echo ""
