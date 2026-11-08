package lab.esb.routes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

/**
 * Queue in, log out — with real Artemis, Kafka and Postgres from Dev Services.
 * One test method on purpose: the three cases share one queue and one topic, and their order matters.
 */
@QuarkusTest
class ScanBridgeRouteTest {

    private static final String SCAN = "<ItemScan><itemTag>0074999999</itemTag><orderId>SO-1001234</orderId>"
            + "<location>SCANNER-1</location><scannedAt>2026-11-08T10:00:00Z</scannedAt></ItemScan>";
    private static final String TOPIC = "kafka:tracking.item-scan.recorded.v1?groupId=test-scans&autoOffsetReset=earliest";

    @Inject
    ProducerTemplate producer;

    @Inject
    ConsumerTemplate consumer;

    @Test
    void scanIsBridgedOnceAndPoisonIsParked() {
        // 1. a valid scan arrives twice (at-least-once delivery from the legacy side)
        producer.sendBody("jms:queue:esb.tracking.scan.in", SCAN);
        producer.sendBody("jms:queue:esb.tracking.scan.in", SCAN);

        String first = consumer.receiveBody(TOPIC, 20_000, String.class);
        assertNotNull(first, "scan did not reach Kafka");
        assertTrue(first.contains("\"itemTag\":\"0074999999\""), first);

        // 2. the duplicate is dropped by the idempotent consumer
        String second = consumer.receiveBody(TOPIC, 5_000, String.class);
        assertNull(second, "duplicate scan was published twice: " + second);

        // 3. a poison message goes to the DLQ unchanged, without retries
        String poison = "<ItemScan><itemTag>ABC</itemTag>";
        producer.sendBody("jms:queue:esb.tracking.scan.in", poison);
        String parked = consumer.receiveBody("jms:queue:esb.tracking.scan.dlq", 10_000, String.class);
        assertEquals(poison, parked);
    }
}
