package org.example;

import com.dxfeed.schedule.Day;
import com.dxfeed.schedule.Schedule;
import com.dxfeed.schedule.Session;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class MarketScheduleUpdaterTest {
    // Map tv.* -> schedule token from schedule_unique.csv (column 1 -> column 4 membership)
    private static final java.util.Map<String, String> SCHEDULE_BY_TV = loadScheduleMap();

    private static java.util.Map<String, String> loadScheduleMap() {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        try (java.io.InputStream in = MarketScheduleUpdaterTest.class.getClassLoader()
                .getResourceAsStream("schedule_unique.csv")) {
            if (in == null) return map; // silently ignore if not present on classpath
            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(in))) {
                String line;
                boolean headerSkipped = false;
                while ((line = br.readLine()) != null) {
                    if (!headerSkipped) { // skip header
                        headerSkipped = true;
                        continue;
                    }
                    String[] parts = line.split(",", -1);
                    if (parts.length < 4) continue;
                    String schedule = parts[0].trim();
                    String tvAll = parts[3].trim();
                    if (schedule.isEmpty() || tvAll.isEmpty()) continue;
                    // tv_all entries are separated by ';'
                    String[] tvs = tvAll.split(";\\s*");
                    for (String tv : tvs) {
                        String key = tv.trim();
                        if (key.isEmpty()) continue;
                        // remove optional @date suffix
                        int at = key.indexOf('@');
                        if (at > 0) key = key.substring(0, at);
                        map.putIfAbsent(key, schedule);
                    }
                }
            }
        } catch (Exception ignore) {
            // If parsing fails, we just won't enrich the print with schedule
        }
        return map;
    }

    private static void printResult(String itemKey, boolean state, Instant now, String dayType) {
        int v = state ? 1 : 0;
        String label = state ? "OPEN" : "CLOSED";
        String schedule = SCHEDULE_BY_TV.getOrDefault(itemKey, "n/a");
        // Use the same 'now' as the computation and show the +/- GRACE window in GMT0 (UTC)
        java.time.Instant minus = now.minus(java.time.Duration.ofMinutes(5));
        java.time.Instant plus = now.plus(java.time.Duration.ofMinutes(5));
        System.out.println("[TEST] itemKey=" + itemKey
                + " value=" + v + " (" + label + ")"
                + " time_gmt0=" + now.toString()
                + " range_gmt0=[" + minus.toString() + ", " + plus.toString() + "]"
                + " day_type=" + dayType
                + " schedule=" + schedule);
    }

    private static java.util.List<String> allTvKeys() {
        java.util.List<String> keys = new java.util.ArrayList<>(SCHEDULE_BY_TV.keySet());
        java.util.Collections.sort(keys);
        return keys;
    }

    private static org.junit.jupiter.api.DynamicTest mkDyn(String tvKey, String displaySuffix,
                                                           boolean dayTrading,
                                                           boolean sPlusTrading,
                                                           boolean sMinusTrading,
                                                           java.lang.Boolean prevState,
                                                           boolean expected) {
        String name = tvKey + " - " + displaySuffix;
        return org.junit.jupiter.api.DynamicTest.dynamicTest(name, () -> {
            MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
            Day day = mock(Day.class);
            when(day.isTrading()).thenReturn(dayTrading);
            when(schedule.getDayByTime(anyLong())).thenReturn(day);

            Session sPlus = mock(Session.class);
            when(sPlus.isTrading()).thenReturn(sPlusTrading);
            Session sMinus = mock(Session.class);
            when(sMinus.isTrading()).thenReturn(sMinusTrading);
            when(schedule.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);

            Instant now = Instant.now();
            boolean state = MarketScheduleUpdater.computeState(schedule, now, prevState);
            String dayType;
            String ds = displaySuffix.toLowerCase(java.util.Locale.ROOT);
            if (ds.contains("holiday")) {
                dayType = "HOLIDAY";
            } else if (ds.contains("short_day")) {
                dayType = "SHORT_DAY";
            } else {
                dayType = "TRADING_DAY";
            }
            printResult(tvKey, state, now, dayType);
            if (expected) {
                assertTrue(state, "Expected OPEN");
            } else {
                assertFalse(state, "Expected CLOSED");
            }
        });
    }

    @org.junit.jupiter.api.TestFactory
    java.util.Collection<org.junit.jupiter.api.DynamicTest> perMarket_holiday_is_always_closed() {
        java.util.List<org.junit.jupiter.api.DynamicTest> out = new java.util.ArrayList<>();
        for (String tv : allTvKeys()) {
            out.add(mkDyn(tv, "holiday_is_closed", false, false, false, true, false));
        }
        return out;
    }

    @org.junit.jupiter.api.TestFactory
    java.util.Collection<org.junit.jupiter.api.DynamicTest> perMarket_plus_window_trading_means_open() {
        java.util.List<org.junit.jupiter.api.DynamicTest> out = new java.util.ArrayList<>();
        for (String tv : allTvKeys()) {
            out.add(mkDyn(tv, "plus_window_trading_means_open", true, true, false, false, true));
        }
        return out;
    }

    @org.junit.jupiter.api.TestFactory
    java.util.Collection<org.junit.jupiter.api.DynamicTest> perMarket_short_day_after_close_is_closed() {
        java.util.List<org.junit.jupiter.api.DynamicTest> out = new java.util.ArrayList<>();
        for (String tv : allTvKeys()) {
            out.add(mkDyn(tv, "short_day_after_close_is_closed", true, false, false, true, false));
        }
        return out;
    }

    @org.junit.jupiter.api.TestFactory
    java.util.Collection<org.junit.jupiter.api.DynamicTest> perMarket_edge_buffer_prev_true_kept() {
        java.util.List<org.junit.jupiter.api.DynamicTest> out = new java.util.ArrayList<>();
        for (String tv : allTvKeys()) {
            out.add(mkDyn(tv, "edge_prev_true_kept", true, false, true, true, true));
        }
        return out;
    }

    @org.junit.jupiter.api.TestFactory
    java.util.Collection<org.junit.jupiter.api.DynamicTest> perMarket_edge_buffer_prev_false_kept() {
        java.util.List<org.junit.jupiter.api.DynamicTest> out = new java.util.ArrayList<>();
        for (String tv : allTvKeys()) {
            out.add(mkDyn(tv, "edge_prev_false_kept", true, false, true, false, false));
        }
        return out;
    }

    @Test
    void holiday_is_always_closed() {
        MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
        Day day = mock(Day.class);
        when(day.isTrading()).thenReturn(false);
        when(schedule.getDayByTime(anyLong())).thenReturn(day);

        Instant now = Instant.now();
        boolean state = MarketScheduleUpdater.computeState(schedule, now, true);
        printResult("tv.XOSL", state, now, "HOLIDAY");
        assertFalse(state, "Holiday/non-trading day must be CLOSED");
    }

    @Test
    void plus_window_trading_means_open() {
        MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
        Day day = mock(Day.class);
        when(day.isTrading()).thenReturn(true);
        when(schedule.getDayByTime(anyLong())).thenReturn(day);

        Session sPlus = mock(Session.class);
        when(sPlus.isTrading()).thenReturn(true);
        Session sMinus = mock(Session.class);
        when(sMinus.isTrading()).thenReturn(false); // won't be used

        when(schedule.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);

        Instant now = Instant.now();
        boolean state = MarketScheduleUpdater.computeState(schedule, now, false);
        printResult("tv.XBER", state, now, "TRADING_DAY");

        assertTrue(state, "If now+grace is trading, state is OPEN");
    }

    @Test
    void minus_window_not_trading_means_closed() {
        MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
        Day day = mock(Day.class);
        when(day.isTrading()).thenReturn(true);
        when(schedule.getDayByTime(anyLong())).thenReturn(day);

        Session sPlus = mock(Session.class);
        when(sPlus.isTrading()).thenReturn(false);
        Session sMinus = mock(Session.class);
        when(sMinus.isTrading()).thenReturn(false);

        when(schedule.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);

        Instant now = Instant.now();
        boolean state = MarketScheduleUpdater.computeState(schedule, now, true);
        printResult("tv.XASX", state, now, "TRADING_DAY");
        assertFalse(state, "If now-grace is not trading, state is CLOSED");
    }

    @Test
    void edge_buffer_uses_previous_state_when_between_windows() {
        // Case 1: previous true should remain true
        MarketScheduleUpdater.ISchedule schedule1 = mock(MarketScheduleUpdater.ISchedule.class);
        Day day1 = mock(Day.class);
        when(day1.isTrading()).thenReturn(true);
        when(schedule1.getDayByTime(anyLong())).thenReturn(day1);
        Session sPlus1 = mock(Session.class);
        when(sPlus1.isTrading()).thenReturn(false);
        Session sMinus1 = mock(Session.class);
        when(sMinus1.isTrading()).thenReturn(true);
        when(schedule1.getSessionByTime(anyLong())).thenReturn(sPlus1, sMinus1);
        Instant now1 = Instant.now();
        boolean prevTrue = MarketScheduleUpdater.computeState(schedule1, now1, true);
        printResult("tv.XSTO", prevTrue, now1, "TRADING_DAY");
        assertTrue(prevTrue, "Edge buffer should keep previous OPEN state");

        // Case 2: previous false should remain false
        MarketScheduleUpdater.ISchedule schedule2 = mock(MarketScheduleUpdater.ISchedule.class);
        Day day2 = mock(Day.class);
        when(day2.isTrading()).thenReturn(true);
        when(schedule2.getDayByTime(anyLong())).thenReturn(day2);
        Session sPlus2 = mock(Session.class);
        when(sPlus2.isTrading()).thenReturn(false);
        Session sMinus2 = mock(Session.class);
        when(sMinus2.isTrading()).thenReturn(true);
        when(schedule2.getSessionByTime(anyLong())).thenReturn(sPlus2, sMinus2);
        Instant now2 = Instant.now();
        boolean prevFalse = MarketScheduleUpdater.computeState(schedule2, now2, false);
        printResult("tv.XWBO", prevFalse, now2, "TRADING_DAY");
        assertFalse(prevFalse, "Edge buffer should keep previous CLOSED state");

        // Case 3: null previous defaults to CLOSED
        MarketScheduleUpdater.ISchedule schedule3 = mock(MarketScheduleUpdater.ISchedule.class);
        Day day3 = mock(Day.class);
        when(day3.isTrading()).thenReturn(true);
        when(schedule3.getDayByTime(anyLong())).thenReturn(day3);
        Session sPlus3 = mock(Session.class);
        when(sPlus3.isTrading()).thenReturn(false);
        Session sMinus3 = mock(Session.class);
        when(sMinus3.isTrading()).thenReturn(true);
        when(schedule3.getSessionByTime(anyLong())).thenReturn(sPlus3, sMinus3);
        Instant now3 = Instant.now();
        boolean prevNull = MarketScheduleUpdater.computeState(schedule3, now3, null);
        printResult("tv.XLON", prevNull, now3, "TRADING_DAY");
        assertFalse(prevNull, "Edge buffer with null prev defaults to CLOSED");
    }

    @Test
    void holiday_even_if_sessions_say_trading_is_closed() {
        MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
        Day day = mock(Day.class);
        when(day.isTrading()).thenReturn(false); // holiday / non-trading day
        when(schedule.getDayByTime(anyLong())).thenReturn(day);

        // Even if sessions would say trading, day check must dominate
        Session sPlus = mock(Session.class);
        when(sPlus.isTrading()).thenReturn(true);
        Session sMinus = mock(Session.class);
        when(sMinus.isTrading()).thenReturn(true);
        when(schedule.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);

        Instant now = Instant.now();
        boolean state = MarketScheduleUpdater.computeState(schedule, now, true);
        printResult("tv.XWAR", state, now, "HOLIDAY");
        assertFalse(state, "Holiday must be CLOSED even if sessions appear trading");
    }

    @Test
    void short_day_after_close_is_closed() {
        // Simulate a short day where we are after early close: both now+grace and now-grace are non-trading
        MarketScheduleUpdater.ISchedule schedule = mock(MarketScheduleUpdater.ISchedule.class);
        Day day = mock(Day.class);
        when(day.isTrading()).thenReturn(true);
        when(schedule.getDayByTime(anyLong())).thenReturn(day);

        Session sPlus = mock(Session.class);
        when(sPlus.isTrading()).thenReturn(false);
        Session sMinus = mock(Session.class);
        when(sMinus.isTrading()).thenReturn(false);
        when(schedule.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);

        Instant now = Instant.now();
        boolean state = MarketScheduleUpdater.computeState(schedule, now, true);
        printResult("tv.XFRA", state, now, "SHORT_DAY");
        assertFalse(state, "After early close on a short day, market should be CLOSED");
    }

    @Test
    void short_day_just_after_ring_but_before_close_uses_prev_state() {
        // Simulate a short day where tPlus is outside trading but tMinus still in trading window
        // Use two separate schedules so Mockito's sequential stubbing does not bleed between calls.
        MarketScheduleUpdater.ISchedule scheduleA = mock(MarketScheduleUpdater.ISchedule.class);
        MarketScheduleUpdater.ISchedule scheduleB = mock(MarketScheduleUpdater.ISchedule.class);

        Day dayA = mock(Day.class);
        when(dayA.isTrading()).thenReturn(true);
        when(scheduleA.getDayByTime(anyLong())).thenReturn(dayA);
        Day dayB = mock(Day.class);
        when(dayB.isTrading()).thenReturn(true);
        when(scheduleB.getDayByTime(anyLong())).thenReturn(dayB);

        Session sPlusA = mock(Session.class);
        when(sPlusA.isTrading()).thenReturn(false); // plus window already after close
        Session sMinusA = mock(Session.class);
        when(sMinusA.isTrading()).thenReturn(true); // minus window still trading
        when(scheduleA.getSessionByTime(anyLong())).thenReturn(sPlusA, sMinusA);

        Session sPlusB = mock(Session.class);
        when(sPlusB.isTrading()).thenReturn(false);
        Session sMinusB = mock(Session.class);
        when(sMinusB.isTrading()).thenReturn(true);
        when(scheduleB.getSessionByTime(anyLong())).thenReturn(sPlusB, sMinusB);

        Instant now = Instant.now();
        boolean prevTrue = MarketScheduleUpdater.computeState(scheduleA, now, true);
        boolean prevFalse = MarketScheduleUpdater.computeState(scheduleB, now, false);
        printResult("tv.CEUX", prevTrue, now, "SHORT_DAY");
        printResult("tv.MTAA", prevFalse, now, "SHORT_DAY");
        assertTrue(prevTrue, "Edge area on short day should keep previous OPEN state");
        assertFalse(prevFalse, "Edge area on short day should keep previous CLOSED state");
    }
}
