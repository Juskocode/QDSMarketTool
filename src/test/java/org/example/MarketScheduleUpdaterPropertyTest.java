package org.example;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests that generate random dates and validate per-minute status file correctness
 * by comparing each minute with computeState using the same deterministic schedule.
 */
public class MarketScheduleUpdaterPropertyTest {

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneOffset.UTC);

    // Reuse deterministic schedule from ZabbixPerMinuteFileTest
    static class WindowSchedule implements MarketScheduleUpdater.ISchedule {
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
            com.dxfeed.schedule.Day day = org.mockito.Mockito.mock(com.dxfeed.schedule.Day.class);
            org.mockito.Mockito.when(day.isTrading()).thenReturn(tradingDay);
            return day;
        }

        @Override
        public com.dxfeed.schedule.Session getSessionByTime(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            LocalTime t = instant.atZone(ZoneOffset.UTC).toLocalTime();
            boolean trading = tradingDay && (t.equals(open) || t.isAfter(open)) && t.isBefore(close);
            com.dxfeed.schedule.Session session = org.mockito.Mockito.mock(com.dxfeed.schedule.Session.class);
            org.mockito.Mockito.when(session.isTrading()).thenReturn(trading);
            return session;
        }
    }

    static class DailyFileGenerator {
        private final Path dir;
        DailyFileGenerator(Path dir) { this.dir = dir; }

        Path fileFor(String tvKey, LocalDate date) {
            String base = "tv_" + tvKey.substring(tvKey.indexOf('.') + 1) + "_market_status_" + date.toString();
            return dir.resolve(base);
        }

        void generate(String tvKey, MarketScheduleUpdater.ISchedule schedule, LocalDate date) throws Exception {
            List<String> lines = new ArrayList<>();
            Boolean prev = null;
            Instant t = date.atStartOfDay().toInstant(ZoneOffset.UTC);
            for (int i = 0; i < 24 * 60; i++) { // full day
                boolean state = MarketScheduleUpdater.computeState(schedule, t, prev);
                prev = state;
                lines.add(TS_FMT.format(t) + " " + (state ? 1 : 0));
                t = t.plus(Duration.ofMinutes(1));
            }
            Path f = fileFor(tvKey, date);
            Files.createDirectories(f.getParent());
            Files.write(f, lines, StandardCharsets.UTF_8);
        }
    }

    @Provide
    Arbitrary<LocalDate> dates() {
        return Arbitraries.integers().between(2024, 2027)
                .flatMap(year -> Arbitraries.integers().between(1, 12).flatMap(month ->
                        Arbitraries.integers().between(1, YearMonth.of(year, month).lengthOfMonth())
                                .map(day -> LocalDate.of(year, month, day))));
    }

    @Provide
    Arbitrary<LocalTime> times() {
        return Arbitraries.integers().between(0, 23).flatMap(h ->
                Arbitraries.integers().between(0, 59).map(m -> LocalTime.of(h, m))
        );
    }

    @Property(tries = 30)
    void generated_daily_file_matches_computeState(@ForAll("dates") LocalDate date,
                                                   @ForAll("times") LocalTime open,
                                                   @ForAll @IntRange(min = 1, max = 600) int durationMinutes,
                                                   @ForAll boolean tradingDay) throws Exception {
        // Ensure close is after open by duration
        LocalTime close = open.plusMinutes(durationMinutes % 720); // keep under ~12h
        if (!close.isAfter(open)) {
            close = open.plusMinutes(1);
        }

        MarketScheduleUpdater.ISchedule schedule = new WindowSchedule(tradingDay, open, close);

        Path dir = Files.createTempDirectory("gen_daily");
        DailyFileGenerator gen = new DailyFileGenerator(dir);
        String tvKey = "tv.TEST";
        gen.generate(tvKey, schedule, date);

        Path f = gen.fileFor(tvKey, date);
        List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
        assertEquals(24 * 60, lines.size());

        // Validate a random subset of minutes (10 samples) against computeState
        Random rnd = new Random(0);
        Boolean prev = null;
        Instant t = date.atStartOfDay().toInstant(ZoneOffset.UTC);
        for (int i = 0; i < lines.size(); i++) {
            boolean state = MarketScheduleUpdater.computeState(schedule, t, prev);
            prev = state;
            String expected = TS_FMT.format(t) + " " + (state ? 1 : 0);
            String actual = lines.get(i).trim();
            assertEquals(expected, actual, "Mismatch at minute index " + i);
            t = t.plus(Duration.ofMinutes(1));
        }
    }
}
