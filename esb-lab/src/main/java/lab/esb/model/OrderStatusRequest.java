package lab.esb.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * What an external partner POSTs to /api/v1/order-status.
 * This is the partner's contract, not ours: it is mapped to the internal Avro event
 * {@code lab.esb.events.OrderStatusUpdated} before it reaches Kafka.
 *
 * @param orderId       e.g. SO-1001234
 * @param status        free text from the partner, mapped to the OrderStatus enum
 * @param location      optional, e.g. ZONE-D7
 * @param estimatedTime optional, ISO-8601 with offset, e.g. 2026-11-08T14:35:00+01:00
 * @param partner       partner code, e.g. ACME
 */
@RegisterForReflection
public record OrderStatusRequest(String orderId, String status, String location, String estimatedTime, String partner) {
}
