package com.dxfeed;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that running main() also generates per-market, per-day, per-minute files
 * next to the --out file, named: <base>_<ID>_<YYYYMMDD>.txt, containing
 * 1440 lines with "HH:mm[:ss] <epochSeconds> <0|1>".
 */
public class MarketScheduleUpdaterPerMinuteMainTest {

    @Test
    void main_generates_per_minute_files_next_to_out() throws Exception {
        Path dir = Files.createTempDirectory("zbx_minute_");
        Path out = dir.resolve("zabbix_sender_input.txt");

        String[] args = new String[] {
                "--defaults", "conf/dxfeed.schedule",
                "--markets", "conf/markets.list",
                "--out", out.toString()
        };

        MarketScheduleUpdater.main(args);

        // Today's date in UTC
        LocalDate todayUtc = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();
        String yyyymmdd = String.format(java.util.Locale.ROOT, "%04d%02d%02d", todayUtc.getYear(), todayUtc.getMonthValue(), todayUtc.getDayOfMonth());

        // From conf/markets.list we expect IDs: CME_GLBX, CME_XCME, CME_CLR
        String base = "zabbix_sender_input";
        for (String id : List.of("CME_GLBX", "CME_XCME", "CME_CLR")) {
            Path f = dir.resolve(base + "_" + id + "_" + yyyymmdd + ".txt");
            assertTrue(Files.exists(f), "Expected per-minute file for " + id + ": " + f);
            List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
            assertEquals(24 * 60, lines.size(), "Expected 1440 lines for " + id);
            // Validate format of a few sample lines
            for (int i = 0; i < lines.size(); i += 143) { // sample ~10 lines
                String l = lines.get(i).trim();
                assertTrue(l.matches("^\\d{2}:\\d{2}(:\\d{2})? \\d+ [01]$"), "Bad line format: " + l);
            }
        }
    }
}
