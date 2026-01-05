package org.example;

import com.dxfeed.schedule.Schedule;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.util.*;

/**
 * GoldenVectorGenerator
 * Generates per-minute golden vectors (0/1) for each tv.*
 * based on schedule_unique.csv and schedule.properties.
 *
 * Usage:
 *   ./gradlew run --args="-Pmain=golden --outDir src/main/resources/out/zabbix/golden --date 2026-01-05"
 * Or run directly:
 *   java -cp ... org.example.GoldenVectorGenerator --outDir <dir> [--date YYYY-MM-DD] [--defaults <dxfeed.schedule>]
 */
public final class GoldenVectorGenerator {

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);

        Path outDir = Paths.get(cli.getOrDefault("--outDir", "src/main/resources/out/zabbix/golden"));
        LocalDate date = cli.containsKey("--date") ? LocalDate.parse(cli.get("--date")) : Instant.now().atZone(ZoneOffset.UTC).toLocalDate();

        // Optionally use external defaults file (not required for token extraction, but accepted for symmetry)
        Path defaultsPath = cli.containsKey("--defaults") ? Paths.get(cli.get("--defaults")) : null;
        if (defaultsPath != null && Files.exists(defaultsPath)) {
            try {
                Schedule.setDefaults(Files.readAllBytes(defaultsPath));
            } catch (Throwable ignore) {
                // Not critical for this generator since we primarily rely on tokens
            }
        }

        // Load token maps
        Map<String, String> csvMap = MarketScheduleUpdater.loadAggregatedSchedules();
        Map<String, String> propsMap = MarketScheduleUpdater.loadSchedulePropertiesTokens(defaultsPath);

        // Write CSV-based golden vectors
        Path csvDir = outDir.resolve("csv");
        int csvFiles = 0;
        for (Map.Entry<String, String> e : csvMap.entrySet()) {
            csvFiles += writeGolden(csvDir, "golden_csv_", e.getKey(), e.getValue(), date);
        }

        // Write schedule.properties-based golden vectors
        Path propsDir = outDir.resolve("props");
        int propsFiles = 0;
        for (Map.Entry<String, String> e : propsMap.entrySet()) {
            propsFiles += writeGolden(propsDir, "golden_props_", e.getKey(), e.getValue(), date);
        }

        System.out.println(Instant.now() + " GOLDEN OK date=" + date + " csv_keys=" + csvMap.size() + " csv_files=" + csvFiles +
                " props_keys=" + propsMap.size() + " props_files=" + propsFiles + " outDir=" + outDir);
    }

    private static int writeGolden(Path baseDir, String prefix, String tvKey, String token, LocalDate dateUtc) {
        try {
            Files.createDirectories(baseDir);
            String bare = tvKey.startsWith("tv.") ? tvKey.substring(3) : tvKey;
            String name = prefix + bare + "_" + String.format(java.util.Locale.ROOT, "%04d%02d%02d", dateUtc.getYear(), dateUtc.getMonthValue(), dateUtc.getDayOfMonth()) + ".txt";
            Path file = baseDir.resolve(name);

            List<String> lines = new ArrayList<>(24 * 60);
            Instant t = dateUtc.atStartOfDay().toInstant(ZoneOffset.UTC);
            Boolean prev = null;
            for (int i = 0; i < 24 * 60; i++) {
                boolean state = MarketScheduleUpdater.computeStateFromCsvToken(token, t, prev);
                prev = state;
                String timeText = t.atZone(ZoneOffset.UTC).toLocalTime().toString();
                if (timeText.length() == 5) timeText += ":00";
                long epoch = t.getEpochSecond();
                lines.add(timeText + " " + epoch + " " + (state ? 1 : 0));
                t = t.plus(Duration.ofMinutes(1));
            }
            Files.write(file, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            return 1;
        } catch (Exception ex) {
            System.out.println(Instant.now() + " GOLDEN WARN tv=" + tvKey + " token=\"" + token + "\" err=" + ex);
            return 0;
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        if (args == null) return m;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                String v = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true";
                m.put(a, v);
            }
        }
        return m;
    }
}
