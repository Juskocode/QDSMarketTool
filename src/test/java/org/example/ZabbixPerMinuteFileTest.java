package org.example;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests a per-minute file writer and reader that produces files like:
 * tv_XOSL_market_status with lines "YYYYMMDD_HHMM <0|1>" (UTC),
 * and a fake sender that reads and "sends" those lines.
 */
public class ZabbixPerMinuteFileTest {

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneOffset.UTC);

    /** Simple deterministic schedule for tests. */
    static final class WindowSchedule implements MarketScheduleUpdater.ISchedule {
        private final boolean tradingDay; // if false -> holiday
        private final LocalTime open;
        private final LocalTime close;

        WindowSchedule(boolean tradingDay, LocalTime open, LocalTime close) {
            this.tradingDay = tradingDay;
            this.open = open;
            this.close = close;
        }

        @Override
        public com.dxfeed.schedule.Day getDayByTime(long millis) {
            com.dxfeed.schedule.Day day = Mockito.mock(com.dxfeed.schedule.Day.class);
            Mockito.when(day.isTrading()).thenReturn(tradingDay);
            return day;
        }

        @Override
        public com.dxfeed.schedule.Session getSessionByTime(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            LocalTime t = instant.atZone(ZoneOffset.UTC).toLocalTime();
            boolean trading = tradingDay && (t.equals(open) || t.isAfter(open)) && t.isBefore(close);
            com.dxfeed.schedule.Session session = Mockito.mock(com.dxfeed.schedule.Session.class);
            Mockito.when(session.isTrading()).thenReturn(trading);
            return session;
        }
    }

    static final class PerMinuteWriter {
        private final Path dir;
        PerMinuteWriter(Path dir) { this.dir = dir; }

        Path fileFor(String tvKey) { return dir.resolve("tv_" + tvKey.substring(tvKey.indexOf('.')+1) + "_market_status"); }

        void writeFile(String tvKey, MarketScheduleUpdater.ISchedule schedule, Instant startInclusiveUtc, int minutes) throws IOException {
            List<String> lines = new ArrayList<>();
            Boolean prev = null;
            Instant t = startInclusiveUtc;
            for (int i = 0; i < minutes; i++) {
                boolean state = MarketScheduleUpdater.computeState(schedule, t, prev);
                prev = state;
                String ts = TS_FMT.format(t);
                lines.add(ts + " " + (state ? 1 : 0));
                t = t.plus(Duration.ofMinutes(1));
            }
            Path f = fileFor(tvKey);
            Files.createDirectories(f.getParent());
            Files.write(f, lines, StandardCharsets.UTF_8);
        }
    }

    static final class PerMinuteReader {
        static List<Record> read(Path f) throws IOException {
            List<Record> out = new ArrayList<>();
            for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) continue;
                String[] parts = t.split("\\s+");
                assertTrue(parts.length >= 2, "Bad line: " + line);
                Instant instant = ZonedDateTime.parse(parts[0].replace('_', 'T') + ":00Z", DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm:ssX")).toInstant();
                int val = Integer.parseInt(parts[1]);
                out.add(new Record(instant, val));
            }
            return out;
        }

        record Record(Instant minute, int value) {}
    }

    static final class FakeSender {
        final List<String> sent = new ArrayList<>();
        void send(Path file) throws IOException {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) continue;
                sent.add(t);
            }
        }
    }

    /**
     * New writer that splits output into one file per UTC day the interval touches.
     * Files are named: tv_<KEY>_market_status_<YYYY-MM-DD>
     */
    static final class PerDayWriter {
        private final Path dir;
        PerDayWriter(Path dir) { this.dir = dir; }

        Path fileFor(String tvKey, LocalDate dateUtc) {
            String base = "tv_" + tvKey.substring(tvKey.indexOf('.')+1) + "_market_status_" + dateUtc;
            return dir.resolve(base);
        }

        void writeFiles(String tvKey, MarketScheduleUpdater.ISchedule schedule, Instant startInclusiveUtc, int minutes) throws IOException {
            Map<LocalDate, List<String>> byDay = new LinkedHashMap<>();
            Boolean prev = null;
            Instant t = startInclusiveUtc;
            for (int i = 0; i < minutes; i++) {
                boolean state = MarketScheduleUpdater.computeState(schedule, t, prev);
                prev = state;
                LocalDate d = t.atZone(ZoneOffset.UTC).toLocalDate();
                byDay.computeIfAbsent(d, k -> new ArrayList<>()).add(TS_FMT.format(t) + " " + (state ? 1 : 0));
                t = t.plus(Duration.ofMinutes(1));
            }
            Files.createDirectories(dir);
            for (Map.Entry<LocalDate, List<String>> e : byDay.entrySet()) {
                Path f = fileFor(tvKey, e.getKey());
                Files.write(f, e.getValue(), StandardCharsets.UTF_8);
                // Print readable logs for visibility
                long opens = e.getValue().stream().filter(s -> s.endsWith(" 1")).count();
                long closes = e.getValue().size() - opens;
                System.out.println("[ZBX_LOG] file=" + f + " date_utc=" + e.getKey()
                        + " lines=" + e.getValue().size() + " open=" + opens + " closed=" + closes
                        + " first_line='" + (e.getValue().isEmpty() ? "" : e.getValue().get(0)) + "'"
                        + " last_line='" + (e.getValue().isEmpty() ? "" : e.getValue().get(e.getValue().size()-1)) + "'");
            }
        }

        void writeFullDay(String tvKey, MarketScheduleUpdater.ISchedule schedule, LocalDate dateUtc) throws IOException {
            Instant start = dateUtc.atStartOfDay().toInstant(ZoneOffset.UTC);
            writeFiles(tvKey, schedule, start, 24 * 60);
        }
    }

    @Test
    void writes_reads_and_fake_sends_per_minute_files() throws Exception {
        Path dir = Files.createTempDirectory("perminute");
        try {
            // Define three TV keys with simple windows
            Map<String, MarketScheduleUpdater.ISchedule> schedules = new LinkedHashMap<>();
            schedules.put("tv.XOSL", new WindowSchedule(true, LocalTime.of(10, 10), LocalTime.of(10, 30))); // long enough window
            schedules.put("tv.CEUX", new WindowSchedule(true, LocalTime.of(9, 0), LocalTime.of(12, 0)));   // broad window
            schedules.put("tv.XFRA", new WindowSchedule(false, LocalTime.of(0, 0), LocalTime.of(0, 0)));    // holiday

            Instant start = Instant.parse("2026-01-03T10:09:00Z");
            int minutes = 4; // 10:09..10:12

            PerMinuteWriter writer = new PerMinuteWriter(dir);
            for (var e : schedules.entrySet()) {
                writer.writeFile(e.getKey(), e.getValue(), start, minutes);
            }

            // Validate content and parse
            for (String tv : schedules.keySet()) {
                Path f = writer.fileFor(tv);
                assertTrue(Files.exists(f), "file exists: " + f);
                List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                assertEquals(minutes, lines.size(), "lines count for " + tv);
                // Validate format and simple invariants
                int ones = 0;
                for (String l : lines) {
                    assertTrue(l.matches("\\d{8}_\\d{4} [01]"), "Bad format: " + l);
                    if (l.endsWith(" 1")) ones++;
                }
                if (tv.equals("tv.XFRA")) {
                    for (String l : lines) assertTrue(l.endsWith(" 0")); // holiday always 0
                } else {
                    assertTrue(ones >= 1, "should have at least one OPEN minute for " + tv);
                }
                // Check reader
                List<PerMinuteReader.Record> recs = PerMinuteReader.read(f);
                assertEquals(minutes, recs.size());
                assertEquals(Instant.parse("2026-01-03T10:09:00Z"), recs.get(0).minute());
            }

            // Fake send
            FakeSender sender = new FakeSender();
            for (String tv : schedules.keySet()) {
                sender.send(writer.fileFor(tv));
            }
            // Total sent lines = 3 files * minutes
            assertEquals(3 * minutes, sender.sent.size());
        } finally {
            // cleanup
            // Files.walk(dir) could be used, but JUnit temp dirs are cleaned by OS; ignore failures
        }
    }

    @Test
    void generates_per_day_files_spanning_midnight_with_logs() throws Exception {
        Path dir = Files.createTempDirectory("perday");
        try {
            PerDayWriter writer = new PerDayWriter(dir);

            // Two schedules: one trading around midnight edge, one holiday
            MarketScheduleUpdater.ISchedule schedTrading = new WindowSchedule(true, LocalTime.of(23, 59), LocalTime.of(0, 2));
            MarketScheduleUpdater.ISchedule schedHoliday = new WindowSchedule(false, LocalTime.MIDNIGHT, LocalTime.MIDNIGHT);

            Instant start = Instant.parse("2026-01-03T23:58:00Z");
            int minutes = 5; // 23:58, 23:59, 00:00, 00:01, 00:02 (spans two days)

            writer.writeFiles("tv.XOSL", schedTrading, start, minutes);
            writer.writeFiles("tv.XFRA", schedHoliday, start, minutes);

            // Validate that two files per tv key were generated
            LocalDate d1 = LocalDate.parse("2026-01-03");
            LocalDate d2 = LocalDate.parse("2026-01-04");

            Path f1 = writer.fileFor("tv.XOSL", d1);
            Path f2 = writer.fileFor("tv.XOSL", d2);
            assertTrue(Files.exists(f1));
            assertTrue(Files.exists(f2));

            List<String> lines1 = Files.readAllLines(f1, StandardCharsets.UTF_8);
            List<String> lines2 = Files.readAllLines(f2, StandardCharsets.UTF_8);

            assertEquals(2, lines1.size(), "first-day minute count");
            assertEquals(3, lines2.size(), "second-day minute count");

            // Check formatting and edge values
            for (String l : lines1) assertTrue(l.matches("\\d{8}_\\d{4} [01]"));
            for (String l : lines2) assertTrue(l.matches("\\d{8}_\\d{4} [01]"));

            // Holiday tv must be all zeros
            Path h1 = writer.fileFor("tv.XFRA", d1);
            Path h2 = writer.fileFor("tv.XFRA", d2);
            for (String l : Files.readAllLines(h1, StandardCharsets.UTF_8)) assertTrue(l.endsWith(" 0"));
            for (String l : Files.readAllLines(h2, StandardCharsets.UTF_8)) assertTrue(l.endsWith(" 0"));
        } finally {
            // no-op
        }
    }

    @Test
    void generates_full_day_file_for_today_with_logs() throws Exception {
        Path dir = Files.createTempDirectory("perday_full");
        try {
            PerDayWriter writer = new PerDayWriter(dir);
            // Use UTC today to avoid locale/timezone differences
            LocalDate todayUtc = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();

            // Trading window in the middle of the day ensures both 0 and 1 values
            MarketScheduleUpdater.ISchedule sched = new WindowSchedule(true, LocalTime.of(10, 0), LocalTime.of(16, 0));

            writer.writeFullDay("tv.CEUX", sched, todayUtc);

            Path f = writer.fileFor("tv.CEUX", todayUtc);
            assertTrue(Files.exists(f));
            List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
            assertEquals(24 * 60, lines.size(), "full-day must have 1440 lines");
            long ones = lines.stream().filter(s -> s.endsWith(" 1")).count();
            long zeros = lines.size() - ones;
            assertTrue(ones > 0 && zeros > 0, "full day should contain both OPEN and CLOSED minutes");

            // Extra logs with a couple of sample lines
            System.out.println("[ZBX_LOG] full_day_file=" + f + " open=" + ones + " closed=" + zeros
                    + " sample_first='" + lines.get(0) + "' sample_mid='" + lines.get(12*60) + "' sample_last='" + lines.get(lines.size()-1) + "'");
        } finally {
            // no-op
        }
    }
}
