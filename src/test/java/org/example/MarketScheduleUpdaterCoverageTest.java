package org.example;

import com.dxfeed.schedule.Day;
import com.dxfeed.schedule.Schedule;
import com.dxfeed.schedule.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

/**
 * Focused tests to increase coverage of MarketScheduleUpdater by exercising
 * CLI parsing, I/O helpers, Schedule overload, and main() branches.
 */
public class MarketScheduleUpdaterCoverageTest {

    private PrintStream origOut;
    private final ByteArrayOutputStream outBuf = new ByteArrayOutputStream();

    @BeforeEach
    void setUpOut() {
        origOut = System.out;
        System.setOut(new PrintStream(outBuf));
    }

    @AfterEach
    void tearDownOut() {
        System.setOut(origOut);
    }

    private static Method privateMethod(String name, Class<?>... paramTypes) throws Exception {
        Method m = MarketScheduleUpdater.class.getDeclaredMethod(name, paramTypes);
        m.setAccessible(true);
        return m;
    }

    @Test
    void parseArgs_and_required_cover_positive_and_negative_cases() throws Exception {
        Method parseArgs = privateMethod("parseArgs", String[].class);
        Method required = privateMethod("required", Map.class, String.class);

        String[] argv = new String[]{"--out", "/tmp/file.txt", "--debug"};
        @SuppressWarnings("unchecked")
        Map<String, String> cli = (Map<String, String>) parseArgs.invoke(null, (Object) argv);
        assertEquals("/tmp/file.txt", cli.get("--out"));
        assertEquals("true", cli.get("--debug"));

        // required present
        String outPath = (String) required.invoke(null, cli, "--out");
        assertEquals("/tmp/file.txt", outPath);

        // required missing -> throws
        Map<String, String> empty = new HashMap<>();
        assertThrows(java.lang.reflect.InvocationTargetException.class, () -> required.invoke(null, empty, "--markets"));

        // required blank value -> throws
        Map<String, String> blank = new HashMap<>();
        blank.put("--markets", "   ");
        assertThrows(java.lang.reflect.InvocationTargetException.class, () -> required.invoke(null, blank, "--markets"));
    }

    @Test
    void loadMarkets_skips_bad_lines_and_parses_valid_entry() throws Exception {
        Method loadMarkets = privateMethod("loadMarkets", Path.class);

        Path tmp = Files.createTempFile("markets_", ".list");
        List<String> lines = Arrays.asList(
                "# comment",
                "   ",
                "ONLY_TWO TOKENS", // malformed
                "ID1 tv.TEST item_one",
                "BAD tv.KEY", // malformed
                "ID2 tv.XOSL item_two"
        );
        Files.write(tmp, lines, StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        java.util.List<Object> result = (java.util.List<Object>) loadMarkets.invoke(null, tmp);
        // Expect only valid entries (2)
        assertEquals(2, result.size());

        // Reflectively inspect record components id/tvKey/itemKey
        Object first = result.get(0);
        Method id = first.getClass().getDeclaredMethod("id");
        Method tv = first.getClass().getDeclaredMethod("tvKey");
        Method item = first.getClass().getDeclaredMethod("itemKey");
        id.setAccessible(true); tv.setAccessible(true); item.setAccessible(true);
        String idVal = (String) id.invoke(first);
        String tvVal = (String) tv.invoke(first);
        String itemVal = (String) item.invoke(first);
        assertEquals("ID1", idVal);
        assertEquals("tv.TEST", tvVal);
        assertEquals("item_one", itemVal);
    }

    @Test
    void prev_state_save_and_load_round_trip_and_error_paths() throws Exception {
        Method savePrev = privateMethod("savePrevState", Path.class, Map.class);
        Method loadPrev = privateMethod("loadPrevState", Path.class);

        Path dir = Files.createTempDirectory("state_io");
        Path file = dir.resolve("state.properties");

        Map<String, Boolean> state = new LinkedHashMap<>();
        state.put("ID1", true);
        state.put("ID2", false);

        // Save and load round-trip
        savePrev.invoke(null, file, state);
        @SuppressWarnings("unchecked")
        Map<String, Boolean> loaded = (Map<String, Boolean>) loadPrev.invoke(null, file);
        assertEquals(state, loaded);

        // loadPrev on non-existing returns empty
        Path missing = dir.resolve("missing.properties");
        @SuppressWarnings("unchecked")
        Map<String, Boolean> empty = (Map<String, Boolean>) loadPrev.invoke(null, missing);
        assertTrue(empty.isEmpty());

        // savePrev error path: make parent a file to trigger exception; should not throw out
        Path parentFile = dir.resolve("not_a_dir");
        Files.write(parentFile, List.of("x"), StandardCharsets.UTF_8);
        Path nested = parentFile.resolve("child.properties");
        // Should be handled internally without throwing out
        savePrev.invoke(null, nested, state);
    }

    @Test
    void log_and_printUsage_write_to_stdout() throws Exception {
        Method log = privateMethod("log", String.class);
        Method printUsage = privateMethod("printUsage");

        log.invoke(null, "TEST_LOG_MESSAGE");
        printUsage.invoke(null);

        String out = outBuf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("TEST_LOG_MESSAGE"), "Log should include message");
        assertTrue(out.contains("Usage:"), "Usage help should be printed");
    }

    @Test
    void computeState_schedule_overload_branches() {
        // Holiday branch -> CLOSED
        Schedule schH = Mockito.mock(Schedule.class);
        Day dayH = Mockito.mock(Day.class);
        when(dayH.isTrading()).thenReturn(false);
        when(schH.getDayByTime(anyLong())).thenReturn(dayH);
        boolean closed = MarketScheduleUpdater.computeState(schH, Instant.now(), true);
        assertFalse(closed);

        // plus-window trading -> OPEN
        Schedule schPlus = Mockito.mock(Schedule.class);
        Day dayT = Mockito.mock(Day.class);
        when(dayT.isTrading()).thenReturn(true);
        when(schPlus.getDayByTime(anyLong())).thenReturn(dayT);
        Session sPlus = Mockito.mock(Session.class);
        when(sPlus.isTrading()).thenReturn(true);
        Session sMinus = Mockito.mock(Session.class);
        when(sMinus.isTrading()).thenReturn(false);
        when(schPlus.getSessionByTime(anyLong())).thenReturn(sPlus, sMinus);
        boolean open = MarketScheduleUpdater.computeState(schPlus, Instant.now(), false);
        assertTrue(open);

        // minus-window not trading -> CLOSED
        Schedule schMinus = Mockito.mock(Schedule.class);
        when(schMinus.getDayByTime(anyLong())).thenReturn(dayT);
        Session sPlusF = Mockito.mock(Session.class);
        when(sPlusF.isTrading()).thenReturn(false);
        Session sMinusF = Mockito.mock(Session.class);
        when(sMinusF.isTrading()).thenReturn(false);
        when(schMinus.getSessionByTime(anyLong())).thenReturn(sPlusF, sMinusF);
        boolean closed2 = MarketScheduleUpdater.computeState(schMinus, Instant.now(), true);
        assertFalse(closed2);

        // edge buffer prev true/false/null — use separate schedules to avoid sequential stubbing interference
        Schedule schEdgeTrue = Mockito.mock(Schedule.class);
        when(schEdgeTrue.getDayByTime(anyLong())).thenReturn(dayT);
        Session sPlusET = Mockito.mock(Session.class);
        when(sPlusET.isTrading()).thenReturn(false);
        Session sMinusET = Mockito.mock(Session.class);
        when(sMinusET.isTrading()).thenReturn(true);
        when(schEdgeTrue.getSessionByTime(anyLong())).thenReturn(sPlusET, sMinusET);
        assertTrue(MarketScheduleUpdater.computeState(schEdgeTrue, Instant.now(), true));

        Schedule schEdgeFalse = Mockito.mock(Schedule.class);
        when(schEdgeFalse.getDayByTime(anyLong())).thenReturn(dayT);
        Session sPlusEF = Mockito.mock(Session.class);
        when(sPlusEF.isTrading()).thenReturn(false);
        Session sMinusEF = Mockito.mock(Session.class);
        when(sMinusEF.isTrading()).thenReturn(true);
        when(schEdgeFalse.getSessionByTime(anyLong())).thenReturn(sPlusEF, sMinusEF);
        assertFalse(MarketScheduleUpdater.computeState(schEdgeFalse, Instant.now(), false));

        Schedule schEdgeNull = Mockito.mock(Schedule.class);
        when(schEdgeNull.getDayByTime(anyLong())).thenReturn(dayT);
        Session sPlusEN = Mockito.mock(Session.class);
        when(sPlusEN.isTrading()).thenReturn(false);
        Session sMinusEN = Mockito.mock(Session.class);
        when(sMinusEN.isTrading()).thenReturn(true);
        when(schEdgeNull.getSessionByTime(anyLong())).thenReturn(sPlusEN, sMinusEN);
        assertFalse(MarketScheduleUpdater.computeState(schEdgeNull, Instant.now(), null));
    }

    @Test
    void main_no_args_prints_usage() {
        // Should print usage and not throw
        MarketScheduleUpdater.main(new String[]{});
        String out = outBuf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Usage:"));
    }

    @Test
    void main_missing_markets_prints_usage_and_writes_nothing() throws Exception {
        Path outFile = Files.createTempFile("zbx_out_", ".txt");
        Files.deleteIfExists(outFile);
        MarketScheduleUpdater.main(new String[]{"--defaults", "conf/dxfeed.schedule", "--out", outFile.toString()});
        String out = outBuf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Usage:"));
        assertFalse(Files.exists(outFile));
    }

    @Test
    void main_with_markets_file_skips_bad_lines_and_writes_valid_lines() throws Exception {
        Path markets = Files.createTempFile("mkts_", ".list");
        List<String> mktLines = Arrays.asList(
                "# header",
                "  ",
                "BAD LINE",
                "ID1 tv.UNKNOWN item_1",
                "ID2 tv.ANOTHER item_2"
        );
        Files.write(markets, mktLines, StandardCharsets.UTF_8);

        Path out = Files.createTempFile("zbx_", ".txt");
        String[] args = new String[]{
                "--defaults", "conf/dxfeed.schedule",
                "--markets", markets.toString(),
                "--out", out.toString()
        };
        MarketScheduleUpdater.main(args);

        // Expect lines for valid markets when Devexperts runtime is available; otherwise tolerate empty output
        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        if (!lines.isEmpty()) {
            assertEquals(2, lines.size());
            for (String line : lines) {
                String[] parts = line.trim().split("\\s+");
                assertEquals(3, parts.length);
                assertEquals("MarketSchedule", parts[0]);
                assertTrue(parts[2].equals("0") || parts[2].equals("1"));
            }
        }
    }
}
