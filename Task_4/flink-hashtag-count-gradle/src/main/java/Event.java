import java.time.Instant;

public class Event {

    private long eventTimeMillis;
    private String lineText;

    public Event() {
    }

    public Event(long eventTimeMillis, String lineText) {
        this.eventTimeMillis = eventTimeMillis;
        this.lineText = lineText;
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    public String getLineText() {
        return lineText;
    }

    public void setLineText(String lineText) {
        this.lineText = lineText;
    }

    public boolean containsHashtagIgnoreCase(String target) {
        if (target == null || target.isEmpty()) {
            return false;
        }
        if (lineText == null || lineText.isEmpty()) {
            return false;
        }

        String normTarget = normalizeHashtag(target);
        if (normTarget.isEmpty()) {
            return false;
        }

        String[] tokens = lineText.split("[\\s,;|]+");
        for (String token : tokens) {
            if (token == null || token.isEmpty()) {
                continue;
            }
            String normToken = normalizeHashtag(token);
            if (normToken.equals(normTarget)) {
                return true;
            }
        }
        return false;
    }

    private String normalizeHashtag(String value) {
        if (value == null) {
            return "";
        }

        String v = value.trim();
        if (v.startsWith("#")) {
            v = v.substring(1);
        }
        // Remove trailing non-alphanumeric/underscore chars (e.g. :,.,!,?)
        v = v.replaceAll("[^A-Za-z0-9_]+$", "");
        return v.toLowerCase();
    }
    
    public Instant getEventTimeInstant() {
        return Instant.ofEpochMilli(eventTimeMillis);
    }

    public static Event of(long eventTimeMillis, String lineText) {
        return new Event(eventTimeMillis, lineText);
    }

}
