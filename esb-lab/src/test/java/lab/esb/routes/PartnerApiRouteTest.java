package lab.esb.routes;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;

import org.apache.camel.ConsumerTemplate;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import lab.esb.events.OrderStatus;
import lab.esb.events.OrderStatusUpdated;

/**
 * Whole app, real HTTP, real Kafka (Dev Services container), mock Schema Registry (mock:// URL).
 * Needs Docker. Proves the contract a partner sees: 202 + an Avro event on the topic, or 400.
 */
@QuarkusTest
class PartnerApiRouteTest {

    @Inject
    ConsumerTemplate consumer;

    @Test
    void validRequestIsAcceptedAndPublishedAsAvro() {
        given().contentType("application/json")
                .body("""
                        {"orderId":"SO-1001234","status":"ready","location":"ZONE-D7","partner":"acme"}""")
                .when().post("/api/v1/order-status")
                .then().statusCode(202).body("eventId", notNullValue());

        OrderStatusUpdated event = consumer.receiveBody(
                "kafka:orders.order-status.updated.v1?groupId=test-partner&autoOffsetReset=earliest"
                        + "&valueDeserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer"
                        + "&schemaRegistryURL=mock://esb-test&specificAvroReader=true",
                20_000, OrderStatusUpdated.class);
        assertNotNull(event, "no event on the topic within 20 s");
        assertEquals("SO-1001234", event.getOrderId());
        assertEquals(OrderStatus.READY, event.getStatus());
    }

    @Test
    void invalidRequestGets400WithReason() {
        given().contentType("application/json")
                .body("""
                        {"orderId":"so-12","status":"ready","partner":"acme"}""")
                .when().post("/api/v1/order-status")
                .then().statusCode(400).body("error", containsString("orderId"));
    }
}
