package com.orderflow;

import java.util.Map;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Tiny wrapper around Jackson (v3 — the one Spring MVC uses in Boot 4, so a
 *  {@link JsonNode} returned from a controller serializes as real nested JSON).
 *  The outbox and event-store code writes and reads JSON payloads constantly;
 *  this keeps that to one line each. Shared by {@code com.orderflow.outbox} and
 *  {@code com.orderflow.es}. */
@Component
public class JsonSupport {

    private final ObjectMapper mapper = new ObjectMapper();

    public String write(Map<String, ?> value) {
        return mapper.writeValueAsString(value);   // Jackson 3: unchecked
    }

    public JsonNode read(String json) {
        return mapper.readTree(json);
    }
}
