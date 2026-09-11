package com.orderflow;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** The mandatory sweeper. Redis never redelivers a pending entry on its own — a
 *  pod that dies mid-message leaves that entry in the PEL forever, and the feed
 *  silently develops holes. This job is the fix:
 *
 *    XPENDING feed dashboard              → who is holding unacked entries, how old
 *    XCLAIM   feed dashboard <me> <idle>  → steal the ones idle past the threshold
 *    (apply + XACK)                       → the hole closes
 *
 *  (XCLAIM after an XPENDING scan is exactly what the XAUTOCLAIM primitive does
 *   in one round trip — Spring Data Redis exposes the two-step form.)
 *
 *  Distributed lock: with more than one orderflow-consumer pod, every pod runs
 *  this @Scheduled method. We do NOT want all of them scanning and racing to
 *  claim. SET lock:feed-sweep <pod> NX EX 30 → exactly one pod sweeps per tick;
 *  the lock auto-expires so a crash can't wedge it. Release is a compare-and-del
 *  Lua script so we only drop a lock we still own. */
@Component
class PelSweeper {

    private static final Logger log = LoggerFactory.getLogger(PelSweeper.class);

    private static final String LOCK = "lock:feed-sweep";
    private static final RedisScript<Long> RELEASE_IF_OWNER = RedisScript.of(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final boolean enabled;
    private final String consumerName;
    private final Duration idle;

    PelSweeper(StringRedisTemplate redis,
               @Value("${orderflow.listeners.redis:true}") boolean enabled,
               @Value("${HOSTNAME:feed-local}") String consumerName,
               @Value("${orderflow.feed.sweeper.idle-ms:60000}") long idleMs) {
        this.redis = redis;
        this.enabled = enabled;
        this.consumerName = consumerName;
        this.idle = Duration.ofMillis(idleMs);
    }

    @Scheduled(fixedDelayString = "${orderflow.feed.sweeper.interval-ms:15000}")
    void sweep() {
        if (!enabled) {
            return;
        }
        Boolean acquired = redis.opsForValue().setIfAbsent(LOCK, consumerName, Duration.ofSeconds(30));
        if (!Boolean.TRUE.equals(acquired)) {
            return;                                   // another pod holds the lock this tick
        }
        try {
            claimStuckEntries();
        } finally {
            redis.execute(RELEASE_IF_OWNER, List.of(LOCK), consumerName);
        }
    }

    private void claimStuckEntries() {
        StreamOperations<String, String, String> ops = redis.opsForStream();

        PendingMessages pending = ops.pending(FeedConsumer.STREAM, FeedConsumer.GROUP,
                Range.unbounded(), 500);

        List<RecordId> stuck = pending.stream()
                .filter(p -> p.getElapsedTimeSinceLastDelivery().compareTo(idle) >= 0)
                .map(PendingMessage::getId)
                .toList();
        if (stuck.isEmpty()) {
            return;
        }

        List<MapRecord<String, String, String>> claimed = ops.claim(
                FeedConsumer.STREAM, FeedConsumer.GROUP, consumerName,
                idle, stuck.toArray(RecordId[]::new));

        for (MapRecord<String, String, String> msg : claimed) {
            FeedStats.record(redis, msg.getValue());
            ops.acknowledge(FeedConsumer.GROUP, msg);
        }
        log.info("sweeper {} adopted {} stuck entries from the PEL (idle >= {}s)",
                consumerName, claimed.size(), idle.toSeconds());
    }
}
