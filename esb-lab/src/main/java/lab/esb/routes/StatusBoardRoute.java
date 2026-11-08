package lab.esb.routes;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

/**
 * The other direction: log in, command out. Reads facts from Kafka and turns the relevant ones into
 *   1. a command on an Artemis queue for a status board system (one handler, per-message ack), and
 *   2. a webhook call to an external partner (WireMock in the lab).
 *
 * kafka orders.order-status.updated.v1 (Avro)
 *   -> filter: only LOCATION_CHANGE and READY matter to the status board
 *   -> jms:queue:esb.status-board.cmd (JSON command)
 *   -> POST partner webhook
 *
 * Note for the design discussion (module 05): steps 1 and 2 are not atomic. If the webhook fails
 * after the JMS send, a retry sends the command again — so the status board must be idempotent
 * (causationId = eventId lets it de-duplicate).
 */
@ApplicationScoped
public class StatusBoardRoute extends RouteBuilder {

    @Override
    public void configure() {
        // Transient failures (partner 503, broker blip): retry with back-off. If still failing,
        // breakOnFirstError=true stops the consumer from committing past this record, so it is
        // re-polled instead of being lost. The price: one bad record blocks its partition.
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(4)
                .redeliveryDelay(1000)
                .useExponentialBackOff()
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        from("kafka:{{esb.topic.order-status}}"
                + "?groupId=esb-status-board"
                + "&autoOffsetReset=earliest"
                + "&valueDeserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer"
                + "&schemaRegistryURL={{esb.schema-registry-url}}"
                + "&specificAvroReader=true"
                + "&breakOnFirstError=true")
                .routeId("status-board-commands")
                .autoStartup("{{esb.status-board.auto-startup:true}}")   // off in tests (module 06)
                .filter(simple("${body.status} in 'LOCATION_CHANGE,READY'"))
                    .bean("orderStatusMapper", "toStatusBoardCommand")
                    .marshal().json()
                    .to("jms:queue:{{esb.queue.status-board-cmd}}")
                    .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                    .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                    .to("{{esb.partner-webhook-url}}?connectTimeout=2000&soTimeout=5000")
                    .log(LoggingLevel.INFO, "Status board + partner notified: ${header.CamelHttpResponseCode}")
                .end();
    }
}
