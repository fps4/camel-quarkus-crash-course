package lab.esb.support;

/** A message that will never succeed, however often it is retried. Goes straight to the DLQ / HTTP 400. */
public class InvalidMessageException extends RuntimeException {
    public InvalidMessageException(String message) {
        super(message);
    }
}
