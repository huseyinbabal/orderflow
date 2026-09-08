package com.orderflow;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** The XADD side of the live feed. One method: append an order moment to the
 *  "feed" stream, always with an approximate MAXLEN cap so Redis can never
 *  grow without bound. This is the "S4: moments" line in {@link OrderController}. */
@Component
class FeedPublisher {

    static final String STREAM = "feed";

    private final StringRedisTemplate redis;
    private final long maxlen;

    FeedPublisher(StringRedisTemplate redis,
                  @Value("${orderflow.feed.maxlen:100000}") long maxlen) {
        this.redis = redis;
        this.maxlen = maxlen;
    }

    RecordId publish(OrderEvent e) {
        MapRecord<String, String, String> record = MapRecord.create(STREAM, Map.of(
                "orderId",    e.orderId(),
                "customerId", e.customerId(),
                "amount",     e.amount().toString(),
                "at",         e.at().toString()));
        // approximateTrimming → Redis trims in whole macro-nodes ("MAXLEN ~"),
        // far cheaper than exact trimming and all a live feed ever needs.
        return redis.opsForStream().add(record,
                XAddOptions.maxlen(maxlen).approximateTrimming(true));
    }
}
