package lab.esb.routes;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

import com.fasterxml.jackson.core.JacksonException;

import lab.esb.model.ItemScan;
import lab.esb.support.InvalidMessageException;

/**
 * Queue in, log out. A legacy system puts XML on an Artemis queue; this route turns each message
 * into a fact on Kafka that many consumers can read and replay.
 *
 * jms:queue:esb.tracking.scan.in (XML, Artemis)
 *   -> unmarshal XML -> validate
 *   -> idempotent consumer (Postgres) — the same scan delivered twice is published once
 *   -> JSON -> Kafka topic tracking.item-scan.recorded.v1, key = itemTag
 *
 * Failures:
 *   - poison message (bad XML, failed validation): no retry, original XML to esb.tracking.scan.dlq
 *   - anything else (Kafka down, DB down): 3 in-memory retries with back-off, then the DLQ
 */
@ApplicationScoped
public class ScanBridgeRoute extends RouteBuilder {

    @Override
    public void configure() {
        errorHandler(deadLetterChannel("jms:queue:{{esb.queue.scan-dlq}}")
                .useOriginalMessage()               // park the XML as received, not a half-transformed body
                .maximumRedeliveries(3)
                .redeliveryDelay(500)
                .backOffMultiplier(2)
                .useExponentialBackOff()
                .retryAttemptedLogLevel(org.apache.camel.LoggingLevel.WARN)
                .logExhausted(true));

        // A retry never fixes a poison message: send it to the DLQ at once.
        onException(InvalidMessageException.class, JacksonException.class)
                .maximumRedeliveries(0);

        // transacted=true: the JMS message is acked only when the route finishes. With the default
        // AUTO_ACKNOWLEDGE, Spring's listener container acks BEFORE the route runs, so a pod crash
        // between ack and Kafka send would lose the scan. A crash now means redelivery, and the
        // idempotent consumer makes the redelivery harmless.
        // cacheLevelName=CACHE_CONSUMER: with transacted=true Spring falls back to CACHE_NONE and
        // opens a new consumer per receive (the console showed 0 consumers between messages).
        from("jms:queue:{{esb.queue.scan-in}}?concurrentConsumers=2&transacted=true&cacheLevelName=CACHE_CONSUMER")
                .routeId("item-scan-bridge")
                .unmarshal().jacksonXml(ItemScan.class)
                .bean("orderStatusMapper", "validate")
                .setHeader(KafkaConstants.KEY, simple("${body.itemTag}"))    // all scans of one item stay in order
                .idempotentConsumer(simple("${body.dedupKey}"))
                    .idempotentRepository("itemScanIdempotentRepo")
                    .skipDuplicate(true)
                    .marshal().json()
                    .log("Item ${header.CamelKafkaKey} -> {{esb.topic.item-scan}}")
                    .to("kafka:{{esb.topic.item-scan}}?requestRequiredAcks=all&enableIdempotence=true")
                .end();
    }
}
