package com.orderflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

/** Per-minute aggregation the dashboard reads. Deliberately trivial: two counters
 *  in Redis, keyed by epoch-minute, TTL'd out after an hour. The point of the slice
 *  is the stream + PEL mechanics, not the analytics — so this stays a helper, not a
 *  service. Called from BOTH the live consumer and the sweeper (adopted messages
 *  must still be counted). */
final class FeedStats {

    private FeedStats() {}

    private static final Duration TTL = Duration.ofHours(1);

    static void record(StringRedisTemplate redis, Map<String, String> event) {
        long minute = Instant.now().getEpochSecond() / 60;
        String orders  = "feed:stats:orders:"  + minute;
        String revenue = "feed:stats:revenue:" + minute;
        redis.opsForValue().increment(orders);
        redis.opsForValue().increment(revenue, Double.parseDouble(event.getOrDefault("amount", "0")));
        redis.expire(orders,  TTL);
        redis.expire(revenue, TTL);
    }

    static long orders(StringRedisTemplate redis) {
        return asLong(redis.opsForValue().get("feed:stats:orders:" + (Instant.now().getEpochSecond() / 60)));
    }

    static double revenue(StringRedisTemplate redis) {
        return asDouble(redis.opsForValue().get("feed:stats:revenue:" + (Instant.now().getEpochSecond() / 60)));
    }

    private static long asLong(String s)   { return s == null ? 0L  : Long.parseLong(s); }
    private static double asDouble(String s){ return s == null ? 0.0 : Double.parseDouble(s); }
}
