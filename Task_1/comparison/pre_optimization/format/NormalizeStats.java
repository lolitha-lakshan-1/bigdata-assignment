import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NormalizeStats {

    public static void main(String[] args) throws Exception {
        Map<String, String> arguments = parseCommandLineArguments(args);

        String inputFilePath = requireArgument(arguments, "in");
        String outputFilePath = requireArgument(arguments, "out");
        String frameworkName = arguments.getOrDefault("framework", "UNKNOWN");
        int stepSeconds = Integer.parseInt(arguments.getOrDefault("step", "5"));

        ParsedInput parsedInput = readAndAggregate(inputFilePath);

        writeNormalizedCsv(
                outputFilePath,
                frameworkName,
                parsedInput.datasetToTimestampAggregations,
                stepSeconds
        );

        System.out.println("Wrote: " + outputFilePath);
    }

    private static ParsedInput readAndAggregate(String inputFilePath) throws IOException {
        java.util.List<String> allLines = Files.readAllLines(Paths.get(inputFilePath), StandardCharsets.UTF_8);
        if (allLines.isEmpty()) {
            throw new IllegalArgumentException("Input file is empty: " + inputFilePath);
        }

        String rawHeaderLine = stripBom(allLines.get(0));
        char delimiter = detectDelimiter(rawHeaderLine);

        String[] headerTokens = splitRow(rawHeaderLine, delimiter);
        Map<String, Integer> headerIndex = buildHeaderIndex(headerTokens);

        int datasetIndex = requireColumn(headerIndex, "dataset");
        int timestampIndex = requireColumn(headerIndex, "timestamp");
        int containerIndex = requireColumn(headerIndex, "container");

        int cpuIndex = headerIndex.getOrDefault("cpu", -1);
        int netIoIndex = headerIndex.getOrDefault("net_io", -1);
        int memIndex = headerIndex.getOrDefault("mem", -1);
        int memUsedTextIndex = headerIndex.getOrDefault("mem_used_text", -1);
        int memUsedMiBIndex = headerIndex.getOrDefault("mem_used_mib", -1);

        Map<String, TreeMap<Long, Aggregation>> datasetToTimestampAggregations = new TreeMap<>();

        for (int lineNumber = 1; lineNumber < allLines.size(); lineNumber++) {
            String line = allLines.get(lineNumber);
            if (line == null) {
                continue;
            }
            String trimmedLine = line.trim();
            if (trimmedLine.isEmpty()) {
                continue;
            }

            String[] fields = splitRow(trimmedLine, delimiter);
            if (fields.length < headerTokens.length) {
                continue;
            }

            String datasetName = fields[datasetIndex].trim();
            long timestampSeconds = parseLongOrZero(fields[timestampIndex]);
            String containerName = fields[containerIndex].trim();

            double cpuPercent = cpuIndex >= 0 ? parsePercent(fields[cpuIndex]) : 0.0;
            double[] netIoMiB = netIoIndex >= 0 ? parseNetIoMiB(fields[netIoIndex]) : new double[]{0.0, 0.0};

            double memoryUsedMiB = parseMemoryUsedMiB(
                    fields,
                    memUsedMiBIndex,
                    memUsedTextIndex,
                    memIndex
            );

            TreeMap<Long, Aggregation> timestampToAggregation =
                    datasetToTimestampAggregations.computeIfAbsent(datasetName, key -> new TreeMap<>());

            Aggregation aggregation =
                    timestampToAggregation.computeIfAbsent(timestampSeconds, key -> new Aggregation());

            aggregation.add(containerName, cpuPercent, netIoMiB[0], netIoMiB[1], memoryUsedMiB);
        }

        return new ParsedInput(datasetToTimestampAggregations);
    }

    private static void writeNormalizedCsv(
            String outputFilePath,
            String frameworkName,
            Map<String, TreeMap<Long, Aggregation>> datasetToTimestampAggregations,
            int stepSeconds
    ) throws IOException {

        Path outputPath = Paths.get(outputFilePath);

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            writer.write("framework,dataset,elapsed_s,cpu_pct_total,net_rx_mib_total,net_tx_mib_total,mem_used_mib_total,containers_in_sum");
            writer.newLine();

            for (Map.Entry<String, TreeMap<Long, Aggregation>> datasetEntry : datasetToTimestampAggregations.entrySet()) {
                String datasetName = datasetEntry.getKey();
                TreeMap<Long, Aggregation> timestampToAggregation = datasetEntry.getValue();

                long syntheticIndex = 0;
                for (Map.Entry<Long, Aggregation> timestampEntry : timestampToAggregation.entrySet()) {
                    long elapsedSeconds = syntheticIndex * (long) stepSeconds;
                    syntheticIndex++;

                    Aggregation aggregation = timestampEntry.getValue();

                    writer.write(csvEscape(frameworkName));
                    writer.write(",");
                    writer.write(csvEscape(datasetName));
                    writer.write(",");
                    writer.write(Long.toString(elapsedSeconds));
                    writer.write(",");
                    writer.write(formatNumber(aggregation.cpuPercentTotal));
                    writer.write(",");
                    writer.write(formatNumber(aggregation.netRxMiBTotal));
                    writer.write(",");
                    writer.write(formatNumber(aggregation.netTxMiBTotal));
                    writer.write(",");
                    writer.write(formatNumber(aggregation.memoryUsedMiBTotal));
                    writer.write(",");
                    writer.write(Integer.toString(aggregation.containerCount));
                    writer.newLine();
                }
            }
        }
    }

    private static double parseMemoryUsedMiB(String[] fields, int memUsedMiBIndex, int memUsedTextIndex, int memIndex) {
        double parsedMiB = Double.NaN;

        if (memUsedMiBIndex >= 0) {
            parsedMiB = parseDoubleOrNaN(fields[memUsedMiBIndex]);
        }
        if (Double.isNaN(parsedMiB) && memUsedTextIndex >= 0) {
            parsedMiB = parseSizeToMiB(fields[memUsedTextIndex]);
        }
        if (Double.isNaN(parsedMiB) && memIndex >= 0) {
            String usedPart = leftSideOfSlash(fields[memIndex]);
            parsedMiB = parseSizeToMiB(usedPart);
        }

        if (Double.isNaN(parsedMiB) || parsedMiB < 0) {
            return 0.0;
        }
        return parsedMiB;
    }

    private static Map<String, String> parseCommandLineArguments(String[] args) {
        Map<String, String> arguments = new HashMap<>();

        for (int index = 0; index < args.length; index++) {
            String token = args[index];
            if (!token.startsWith("--")) {
                continue;
            }
            String key = token.substring(2);
            String value = "true";

            if (index + 1 < args.length && !args[index + 1].startsWith("--")) {
                value = args[index + 1];
                index++;
            }

            arguments.put(key, value);
        }

        return arguments;
    }

    private static String requireArgument(Map<String, String> arguments, String name) {
        String value = arguments.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required argument: --" + name);
        }
        return value.trim();
    }

    private static char detectDelimiter(String headerLine) {
        if (headerLine.indexOf('\t') >= 0) return '\t';
        if (headerLine.indexOf(',') >= 0) return ',';
        if (headerLine.indexOf(';') >= 0) return ';';
        return ',';
    }

    private static String[] splitRow(String line, char delimiter) {
        return line.split(Pattern.quote(String.valueOf(delimiter)), -1);
    }

    private static Map<String, Integer> buildHeaderIndex(String[] headers) {
        Map<String, Integer> headerIndex = new HashMap<>();
        for (int index = 0; index < headers.length; index++) {
            String normalizedHeader = stripBom(headers[index]).trim();
            headerIndex.put(normalizedHeader, index);
        }
        return headerIndex;
    }

    private static int requireColumn(Map<String, Integer> headerIndex, String columnName) {
        Integer index = headerIndex.get(columnName);
        if (index == null) {
            throw new IllegalArgumentException("Missing column: " + columnName + ". Found: " + headerIndex.keySet());
        }
        return index;
    }

    private static long parseLongOrZero(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static double parseDoubleOrNaN(String value) {
        try {
            String trimmed = value.trim();
            if (trimmed.isEmpty()) return Double.NaN;
            return Double.parseDouble(trimmed);
        } catch (Exception ignored) {
            return Double.NaN;
        }
    }

    private static double parsePercent(String cpuValue) {
        String trimmed = cpuValue.trim().replace("%", "");
        if (trimmed.isEmpty()) return 0.0;
        return Double.parseDouble(trimmed);
    }

    private static double[] parseNetIoMiB(String netIoValue) {
        String[] parts = netIoValue.split("/");
        if (parts.length < 2) {
            return new double[]{0.0, 0.0};
        }

        double receivedMiB = parseSizeToMiB(parts[0].trim());
        double transmittedMiB = parseSizeToMiB(parts[1].trim());

        if (Double.isNaN(receivedMiB)) receivedMiB = 0.0;
        if (Double.isNaN(transmittedMiB)) transmittedMiB = 0.0;

        return new double[]{receivedMiB, transmittedMiB};
    }

    private static String leftSideOfSlash(String value) {
        int slashIndex = value.indexOf('/');
        if (slashIndex < 0) {
            return value.trim();
        }
        return value.substring(0, slashIndex).trim();
    }

    private static final Pattern SIZE_PATTERN = Pattern.compile("^\\s*([0-9]*\\.?[0-9]+)\\s*([A-Za-z]+)\\s*$");

    private static double parseSizeToMiB(String token) {
        String trimmed = token.trim();
        if (trimmed.isEmpty()) return Double.NaN;

        Matcher matcher = SIZE_PATTERN.matcher(trimmed);
        if (!matcher.matches()) return Double.NaN;

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2);

        double bytes = sizeToBytes(value, unit);
        if (Double.isNaN(bytes)) return Double.NaN;

        return bytes / (1024d * 1024d);
    }

    private static double sizeToBytes(double value, String unit) {
        switch (unit) {
            case "B":
                return value;

            case "kB":
            case "KB":
                return value * 1000d;
            case "MB":
                return value * 1000d * 1000d;
            case "GB":
                return value * 1000d * 1000d * 1000d;
            case "TB":
                return value * 1000d * 1000d * 1000d * 1000d;

            case "KiB":
                return value * 1024d;
            case "MiB":
                return value * 1024d * 1024d;
            case "GiB":
                return value * 1024d * 1024d * 1024d;
            case "TiB":
                return value * 1024d * 1024d * 1024d * 1024d;

            default:
                return Double.NaN;
        }
    }

    private static String formatNumber(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String csvEscape(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static String stripBom(String text) {
        if (text == null) return null;
        return text.replace("\uFEFF", "");
    }

    private static class ParsedInput {
        final Map<String, TreeMap<Long, Aggregation>> datasetToTimestampAggregations;

        ParsedInput(Map<String, TreeMap<Long, Aggregation>> datasetToTimestampAggregations) {
            this.datasetToTimestampAggregations = datasetToTimestampAggregations;
        }
    }

    private static class Aggregation {
        double cpuPercentTotal = 0.0;
        double netRxMiBTotal = 0.0;
        double netTxMiBTotal = 0.0;
        double memoryUsedMiBTotal = 0.0;
        int containerCount = 0;

        void add(String containerName, double cpuPercent, double netRxMiB, double netTxMiB, double memoryUsedMiB) {
            this.cpuPercentTotal += cpuPercent;
            this.netRxMiBTotal += netRxMiB;
            this.netTxMiBTotal += netTxMiB;
            this.memoryUsedMiBTotal += memoryUsedMiB;
            this.containerCount += 1;
        }
    }
}
