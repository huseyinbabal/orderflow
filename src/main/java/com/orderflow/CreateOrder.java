package com.orderflow;

import java.math.BigDecimal;

public record CreateOrder(String orderId, String customerId, BigDecimal amount) {}
