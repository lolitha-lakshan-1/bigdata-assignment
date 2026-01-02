import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventParser {

    private static final int COL_DATE_POSTED = 4;

    // Matches timestamps like 2024-12-15T22:51:08.000Z anywhere in the line
    private static final Pattern ISO_INSTANT_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z");

    public static Event parse(String line) {
        if (line == null) {
            return null;
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        String[] cols = splitCsvLine(trimmed);
        if (cols.length == 0) {
            return null;
        }

        // Skip header row
        if ("id".equalsIgnoreCase(cleanField(cols[0]))) {
            return null;
        }

        long eventTimeMillis = extractEventTimeFromLine(trimmed);

        if (eventTimeMillis == 0L && cols.length > COL_DATE_POSTED) {
            eventTimeMillis = parseEventTime(cols[COL_DATE_POSTED]);
        }

        String textForSearch = cleanField(trimmed);
        if (textForSearch.isEmpty()) {
            return null;
        }

        return Event.of(eventTimeMillis, textForSearch);
    }

    private static long extractEventTimeFromLine(String line) {
        Matcher matcher = ISO_INSTANT_PATTERN.matcher(line);
        if (matcher.find()) {
            String ts = matcher.group();
            try {
                return Instant.parse(ts).toEpochMilli();
            } catch (Exception e) {

            }
        }

        return 0L;
    }

    private static long parseEventTime(String rawDate) {
        String cleaned = cleanField(rawDate);
        if (cleaned.isEmpty()) {
            return 0L;
        }

        try {
            return Instant.parse(cleaned).toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }

    private static String[] splitCsvLine(String line) {
        List<String> columns = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                columns.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        columns.add(current.toString());
        return columns.toArray(new String[0]);
    }

    private static String cleanField(String value) {
        if (value == null) {
            return "";
        }
        String withoutQuotes = value.replace("\"", "");
        return withoutQuotes.trim();
    }
}
