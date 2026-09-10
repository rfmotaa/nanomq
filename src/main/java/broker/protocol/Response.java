package broker.protocol;

public class Response {
    private final String status;
    private final String message;
    private final Long offset;       
    private final Long nextOffset; 
    private final Boolean endOfLog; 

    private Response(String status, String message, Long offset, Long nextOffset, Boolean endOfLog) {
        this.status = status;
        this.message = message;
        this.offset = offset;
        this.nextOffset = nextOffset;
        this.endOfLog = endOfLog;
    }

    public static Response ok(String message) {
        return new Response("OK", message, null, null, null);
    }

    public static Response stored(long offset) {
        return new Response("OK", "stored at offset " + offset, offset, offset + 1, null);
    }

    public static Response record(long offset, String payload) {
        return new Response("OK", payload, offset, offset + 1, false);
    }

    public static Response endOfLog(long offset) {
        return new Response("OK", null, null, offset, true);
    }

    public static Response error(String message) {
        return new Response("ERROR", message, null, null, null);
    }
}
