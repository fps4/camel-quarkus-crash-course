package lab.esb.support;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lab.esb.events.OrderStatus;
import lab.esb.events.OrderStatusUpdated;
import lab.esb.model.ItemScan;
import lab.esb.model.OrderStatusRequest;

/**
 * Plain Java mapping and validation. Kept out of the route so it can be unit-tested without Camel.
 * Throws {@link InvalidMessageException} for anything a retry will not fix.
 */
@ApplicationScoped
@Named("orderStatusMapper")
@RegisterForReflection
public class OrderStatusMapper {

    /** Partner request (their contract) -> internal Avro event (our contract). */
    public OrderStatusUpdated toEvent(OrderStatusRequest in) {
        if (in == null) {
            throw new InvalidMessageException("empty body");
        }
        if (in.orderId() == null || !in.orderId().matches("[A-Z]{2,4}-\\d{4,10}")) {
            throw new InvalidMessageException("orderId must look like SO-1001234, got: " + in.orderId());
        }
        if (in.partner() == null || in.partner().isBlank()) {
            throw new InvalidMessageException("partner is required");
        }
        return OrderStatusUpdated.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(in.orderId())
                .setStatus(mapStatus(in.status()))
                .setLocation(blankToNull(in.location()))
                .setEstimatedTime(parseTime(in.estimatedTime()))
                .setSource(in.partner().toUpperCase(Locale.ROOT))
                .setOccurredAt(Instant.now())
                .build();
    }

    /** Internal event -> command for the status board system (a queue consumer, one handler per message). */
    public Map<String, Object> toStatusBoardCommand(OrderStatusUpdated e) {
        return Map.of(
                "command", "UPDATE_STATUS_BOARD",
                "orderId", e.getOrderId(),
                "status", e.getStatus().name(),
                "location", e.getLocation() == null ? "" : e.getLocation(),
                "causationId", e.getEventId(),
                "issuedAt", Instant.now().toString());
    }

    public void validate(ItemScan scan) {
        if (scan == null || isBlank(scan.getItemTag()) || !scan.getItemTag().matches("\\d{10}")) {
            throw new InvalidMessageException("itemTag must be 10 digits");
        }
        if (isBlank(scan.getOrderId()) || isBlank(scan.getLocation()) || isBlank(scan.getScannedAt())) {
            throw new InvalidMessageException("orderId, location and scannedAt are required");
        }
    }

    private static OrderStatus mapStatus(String s) {
        if (s == null) {
            throw new InvalidMessageException("status is required");
        }
        String norm = s.trim().toUpperCase(Locale.ROOT).replace(' ', '_').replace('-', '_');
        return switch (norm) {
            case "CREATED", "NEW" -> OrderStatus.CREATED;
            case "CONFIRMED", "ACCEPTED" -> OrderStatus.CONFIRMED;
            case "DELAYED" -> OrderStatus.DELAYED;
            case "LOCATION_CHANGE", "LOCATION_CHANGED", "MOVED" -> OrderStatus.LOCATION_CHANGE;
            case "READY", "READY_FOR_PICKUP" -> OrderStatus.READY;
            case "DISPATCHED", "SHIPPED" -> OrderStatus.DISPATCHED;
            case "CANCELLED", "CANCELED" -> OrderStatus.CANCELLED;
            default -> throw new InvalidMessageException("unknown status: " + s);
        };
    }

    private static Instant parseTime(String s) {
        if (isBlank(s)) {
            return null;
        }
        try {
            return OffsetDateTime.parse(s).toInstant();
        } catch (DateTimeParseException ex) {
            throw new InvalidMessageException("estimatedTime must be ISO-8601 with offset, got: " + s);
        }
    }

    private static String blankToNull(String s) {
        return isBlank(s) ? null : s.trim();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
