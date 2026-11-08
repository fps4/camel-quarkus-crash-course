package lab.esb.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * An item scan as a legacy system puts it on the Artemis queue, as XML:
 *
 * <pre>{@code
 * <ItemScan>
 *   <itemTag>0074123456</itemTag>
 *   <orderId>SO-1001234</orderId>
 *   <location>SCANNER-3</location>
 *   <scannedAt>2026-11-08T13:05:12Z</scannedAt>
 * </ItemScan>
 * }</pre>
 *
 * Getters, not public fields: Camel's Simple language (${body.itemTag}) calls getItemTag() and
 * cannot read a public field. Found the hard way while testing this lab.
 */
@RegisterForReflection
@JacksonXmlRootElement(localName = "ItemScan")
public class ItemScan {
    private String itemTag;
    private String orderId;
    private String location;
    private String scannedAt;

    public String getItemTag() { return itemTag; }
    public void setItemTag(String itemTag) { this.itemTag = itemTag; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    public String getScannedAt() { return scannedAt; }
    public void setScannedAt(String scannedAt) { this.scannedAt = scannedAt; }

    /**
     * Business key used by the idempotent consumer: the same scan delivered twice is one fact.
     * Not named getXxx, so Jackson does not write it into the JSON.
     */
    public String dedupKey() {
        return itemTag + "|" + location + "|" + scannedAt;
    }
}
