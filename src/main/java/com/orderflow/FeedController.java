package com.orderflow;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** The dashboard endpoint. Reads ONLY from Redis — stream length, PEL depth,
 *  this-minute counters, and the last N entries (XREVRANGE). Sub-millisecond,
 *  no database touched. This is the slice's payoff: the data that would have
 *  cost a Kafka cluster is one GET against memory. */
@RestController
class FeedController {

    private final StringRedisTemplate redis;

    FeedController(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @GetMapping("/feed/live")
    Map<String, Object> live(@RequestParam(defaultValue = "20") int n) {
        StreamOperations<String, String, String> ops = redis.opsForStream();

        Long length = ops.size(FeedConsumer.STREAM);
        List<MapRecord<String, String, String>> recent = ops.reverseRange(
                FeedConsumer.STREAM, Range.unbounded(), Limit.limit().count(n));

        return Map.of(
                "streamLength",      length == null ? 0L : length,
                "pending",           pendingCount(ops),
                "ordersThisMinute",  FeedStats.orders(redis),
                "revenueThisMinute", FeedStats.revenue(redis),
                "recent",            recent == null ? List.of() : recent.stream().map(Record::getValue).toList());
    }

    private long pendingCount(StreamOperations<String, String, String> ops) {
        try {
            PendingMessagesSummary summary = ops.pending(FeedConsumer.STREAM, FeedConsumer.GROUP);
            return summary == null ? 0L : summary.getTotalPendingMessages();
        } catch (RuntimeException noGroupYet) {
            return 0L;                          // group not created until the first consumer starts
        }
    }
}
