package lab.esb.routes;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.rest.RestBindingMode;

import lab.esb.model.OrderStatusRequest;
import lab.esb.support.InvalidMessageException;

/**
 * Inbound channel for external partners: HTTP in, Kafka out.
 *
 * POST /api/v1/order-status (partner JSON)
 *   -> validate + map to the internal Avro event (OrderStatusMapper)
 *   -> Kafka topic orders.order-status.updated.v1, key = orderId, Avro via Schema Registry sr-a
 *   -> 202 Accepted with the eventId
 *
 * Patterns: Messaging Gateway, Message Translator, canonical data model at the boundary.
 */
@ApplicationScoped
public class PartnerApiRoute extends RouteBuilder {

    @Override
    public void configure() {
        // Bad input is the caller's problem: 400, no retry. Scoped to the routes in this class.
        onException(InvalidMessageException.class)
                .handled(true)
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(e -> Map.of("error", e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class).getMessage()))
                // REST binding skips error responses (skipBindingOnErrorCode=true), so marshal ourselves.
                .marshal().json();

        // Kafka down or slow: 503 so the partner retries later. We never answer 202 for an event we did not store.
        onException(Exception.class)
                .handled(true)
                .log("Could not publish order status: ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(503))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(e -> Map.of("error", "temporarily unavailable, retry later"))
                .marshal().json();

        restConfiguration()
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "false");

        rest("/api/v1")
                .post("/order-status")
                    .description("Partner reports an order status change")
                    .consumes("application/json")
                    .produces("application/json")
                    .type(OrderStatusRequest.class)
                    .to("direct:publish-order-status");

        from("direct:publish-order-status")
                .routeId("partner-order-status")
                .bean("orderStatusMapper", "toEvent")                       // OrderStatusRequest -> OrderStatusUpdated (Avro)
                .setProperty("eventId", simple("${body.eventId}"))
                .setHeader(KafkaConstants.KEY, simple("${body.orderId}"))    // same order -> same partition -> ordered
                .to("kafka:{{esb.topic.order-status}}"
                        + "?valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer"
                        + "&schemaRegistryURL={{esb.schema-registry-url}}"
                        + "&requestRequiredAcks=all"
                        + "&enableIdempotence=true")
                .log("Published ${exchangeProperty.eventId} for ${body.orderId}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
                .setBody(e -> Map.of("status", "accepted", "eventId", e.getProperty("eventId", String.class)));
    }
}
