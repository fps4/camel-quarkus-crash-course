package lab.esb.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import lab.esb.events.OrderStatus;
import lab.esb.events.OrderStatusUpdated;
import lab.esb.model.OrderStatusRequest;

/** Plain JUnit, no Quarkus, no Camel: runs in milliseconds. This is why mapping lives in a bean. */
class OrderStatusMapperTest {

    private final OrderStatusMapper mapper = new OrderStatusMapper();

    @Test
    void mapsPartnerWordingToCanonicalStatus() {
        OrderStatusUpdated e = mapper.toEvent(
                new OrderStatusRequest("SO-1001234", "location change", "ZONE-D7", "2026-11-08T14:35:00+01:00", "acme"));
        assertEquals(OrderStatus.LOCATION_CHANGE, e.getStatus());
        assertEquals("ACME", e.getSource());
        assertEquals("ZONE-D7", e.getLocation());
        assertEquals("2026-11-08T13:35:00Z", e.getEstimatedTime().toString());
    }

    @Test
    void blankLocationBecomesNull() {
        OrderStatusUpdated e = mapper.toEvent(new OrderStatusRequest("SO-2002345", "delayed", " ", null, "globex"));
        assertNull(e.getLocation());
        assertNull(e.getEstimatedTime());
    }

    @Test
    void rejectsBadOrderIdAndUnknownStatus() {
        assertThrows(InvalidMessageException.class,
                () -> mapper.toEvent(new OrderStatusRequest("so-12", "ready", null, null, "acme")));
        assertThrows(InvalidMessageException.class,
                () -> mapper.toEvent(new OrderStatusRequest("SO-1001234", "flying sideways", null, null, "acme")));
    }
}
