#!/usr/bin/env bash
set -eo pipefail

BASE_URL="http://localhost:8000"
EVENT_ID="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
OUTPUT_DIR="/tmp/reservation_test"
RESULTS_FILE="${OUTPUT_DIR}/status_codes.log"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo -e "${YELLOW}=== Pre-Test: Checking API Health ===${NC}"
if ! curl -sf "${BASE_URL}/docs" > /dev/null; then
    echo -e "${RED}Error: Cannot reach API at ${BASE_URL}.${NC}"
    exit 1
fi
echo -e "${GREEN}API is reachable.${NC}\n"

# ------------------------------------------------------------------------------
# TEST 1: Single Reservation Hold (Happy Path)
# ------------------------------------------------------------------------------
echo -e "${YELLOW}=== Test 1: Single Reservation Hold (Happy Path) ===${NC}"
docker compose exec -T redis redis-cli SET "event:${EVENT_ID}:available" 100 > /dev/null

TEST_USER_ID="11111111-1111-1111-1111-111111111111"
HOLD_PAYLOAD="{\"event_id\":\"${EVENT_ID}\",\"user_id\":\"${TEST_USER_ID}\",\"seats\":2}"

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/reservations/hold" \
  -H "Content-Type: application/json" \
  -d "$HOLD_PAYLOAD")

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -eq 201 ]; then
    echo -e "${GREEN}[PASS] Received HTTP 201 Created${NC}"
else
    echo -e "${RED}[FAIL] Expected HTTP 201, got ${HTTP_STATUS}${NC}"
    echo "Response body: $BODY"
    exit 1
fi
echo ""

# ------------------------------------------------------------------------------
# TEST 2: Concurrency Stress Test (Oversell Prevention)
# ------------------------------------------------------------------------------
echo -e "${YELLOW}=== Test 2: Concurrency Stress Test (Oversell Prevention) ===${NC}"

AVAILABLE_SEATS=10
TOTAL_REQUESTS=50
CONCURRENCY=50

echo "1. Resetting Redis inventory to exactly ${AVAILABLE_SEATS} seats..."
docker compose exec -T redis redis-cli SET "event:${EVENT_ID}:available" "$AVAILABLE_SEATS" > /dev/null

echo "2. Firing ${TOTAL_REQUESTS} concurrent requests (-P ${CONCURRENCY}) for 1 seat each..."

export BASE_URL EVENT_ID OUTPUT_DIR

run_request() {
  local req_num=$1
  local user_uuid
  user_uuid=$(printf "00000000-0000-0000-0000-%012d" "$req_num")
  local payload="{\"event_id\":\"${EVENT_ID}\",\"user_id\":\"${user_uuid}\",\"seats\":1}"

  # Write output to an isolated per-request file to prevent clobbering
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST "${BASE_URL}/api/reservations/hold" \
    -H "Content-Type: application/json" \
    -d "$payload" > "${OUTPUT_DIR}/req_${req_num}.log"
}

export -f run_request

# Clean xargs invocation without conflicting flags
seq 1 "$TOTAL_REQUESTS" | xargs -P "$CONCURRENCY" -I {} bash -c 'run_request "$@"' _ {}

echo "3. Collating results from ${TOTAL_REQUESTS} workers..."
cat "${OUTPUT_DIR}"/req_*.log > "$RESULTS_FILE"

TOTAL_RECORDED=$(wc -l < "$RESULTS_FILE" | tr -d ' ')
COUNT_201=$(grep -c "201" "$RESULTS_FILE" || true)
COUNT_409=$(grep -c "409" "$RESULTS_FILE" || true)

echo "--------------------------------------------------"
echo "Total Requests Processed: ${TOTAL_RECORDED} / ${TOTAL_REQUESTS}"
echo "Status Code Breakdown:"
sort "$RESULTS_FILE" | uniq -c
echo "--------------------------------------------------"

FAILED=0

if [ "$COUNT_201" -eq "$AVAILABLE_SEATS" ]; then
    echo -e "${GREEN}[PASS] Exactly ${AVAILABLE_SEATS} requests were granted holds (HTTP 201).${NC}"
else
    echo -e "${RED}[FAIL] Expected exactly ${AVAILABLE_SEATS} holds, but got ${COUNT_201}.${NC}"
    FAILED=1
fi

EXPECTED_CONFLICTS=$((TOTAL_REQUESTS - AVAILABLE_SEATS))
if [ "$COUNT_409" -eq "$EXPECTED_CONFLICTS" ]; then
    echo -e "${GREEN}[PASS] Exactly ${EXPECTED_CONFLICTS} requests were rejected with HTTP 409 Conflict.${NC}"
else
    echo -e "${RED}[FAIL] Expected ${EXPECTED_CONFLICTS} conflicts, but got ${COUNT_409}.${NC}"
    FAILED=1
fi

FINAL_REDIS_SEATS=$(docker compose exec -T redis redis-cli GET "event:${EVENT_ID}:available" | tr -d '\r')
echo "4. Final Redis seats remaining: ${FINAL_REDIS_SEATS}"

if [ "$FINAL_REDIS_SEATS" -eq 0 ]; then
    echo -e "${GREEN}[PASS] Redis counter reached exactly 0 without falling negative.${NC}"
else
    echo -e "${RED}[FAIL] Expected Redis counter to be 0, found ${FINAL_REDIS_SEATS}.${NC}"
    FAILED=1
fi

rm -rf "$OUTPUT_DIR"

if [ "$FAILED" -eq 0 ]; then
    echo -e "\n${GREEN}✓ All race-condition and reservation tests PASSED!${NC}"
    exit 0
else
    echo -e "\n${RED}✗ Concurrency tests FAILED.${NC}"
    exit 1
fi