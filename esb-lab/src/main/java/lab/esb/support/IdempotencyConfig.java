package lab.esb.support;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;

import org.apache.camel.processor.idempotent.jdbc.JdbcMessageIdRepository;

/**
 * The idempotent-consumer store. Kept in Postgres, not in memory, so a restart or a second pod
 * still recognises a message it has already processed.
 * JdbcMessageIdRepository creates table CAMEL_MESSAGEPROCESSED on first start.
 */
public class IdempotencyConfig {

    @Produces
    @ApplicationScoped
    @Named("itemScanIdempotentRepo")
    JdbcMessageIdRepository itemScanIdempotentRepo(DataSource dataSource) {
        // "processorName" scopes the keys, so several routes can share one table.
        return new JdbcMessageIdRepository(dataSource, "item-scan-bridge");
    }
}
