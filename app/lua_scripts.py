RESERVE_SEATS_LUA = """
local event_key = KEYS[1]
local reservation_key = KEYS[2]
local requested_seats = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])
local payload = ARGV[3]

local available = tonumber(redis.call('GET', event_key) or '-1')

if available == -1 then
    return -1 -- Event not initialized
end

if available >= requested_seats then
    redis.call('DECRBY', event_key, requested_seats)
    redis.call('SET', reservation_key, payload, 'EX', ttl_seconds)
    return 1 -- Success
else
    return 0 -- Insufficient inventory
end
"""