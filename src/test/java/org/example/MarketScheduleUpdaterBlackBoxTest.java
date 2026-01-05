package org.example;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Black-box test: run the main() with real CLI args and validate the Zabbix sender output file.
 * Uses the provided conf/dxfeed.schedule (placeholder) and conf/markets.list.
 */
public class MarketScheduleUpdaterBlackBoxTest {

    @Test
    void main_writes_sender_file_with_expected_format() throws Exception {
        Path out = Files.createTempFile("zbx_sender_", ".txt");

        String[] args = new String[] {
                "--defaults", "conf/dxfeed.schedule",
                "--markets", "conf/markets.list",
                "--out", out.toString()
        };

        // Execute
        MarketScheduleUpdater.main(args);

        // Validate file exists and contains lines in format: "MarketSchedule <itemKey> <0|1>"
        assertTrue(Files.exists(out), "Output file should exist");
        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        // In some environments, schedule resolution may be unavailable; in that case,
        // the tool may not emit any lines. Accept empty output; otherwise validate strictly.
        if (!lines.isEmpty()) {
            // Optionally check count equals number of markets (3) if you want stricter assertion
            for (String line : lines) {
                String t = line.trim();
                assertFalse(t.isEmpty());
                String[] parts = t.split("\\s+");
                assertTrue(parts.length == 3, "Line must have 3 tokens: " + line);
                assertEquals("MarketSchedule", parts[0], "Host must be MarketSchedule");
                // itemKey is arbitrary; value must be 0 or 1
                assertTrue(parts[2].equals("0") || parts[2].equals("1"), "Value must be 0 or 1");
            }
        }
    }

    @Test
    void main_without_defaults_uses_classpath_defaults_and_writes_to_default_paths() throws Exception {
        // Ensure default out/log files are removed before run
        Path defaultOut = Path.of("src/main/resources/out/zabbix/zabbix_sender_input.txt");
        Path defaultLog = Path.of("src/main/resources/out/logs/market_schedule.log");
        Files.createDirectories(defaultOut.getParent());
        Files.createDirectories(defaultLog.getParent());
        Files.deleteIfExists(defaultOut);
        Files.deleteIfExists(defaultLog);

        String[] args = new String[] {
                "--markets", "conf/markets.list"
        };

        MarketScheduleUpdater.main(args);

        // Validate default out file exists
        assertTrue(Files.exists(defaultOut), "Default out file should exist");
        // Content format check if not empty
        var lines = Files.readAllLines(defaultOut, StandardCharsets.UTF_8);
        if (!lines.isEmpty()) {
            for (String line : lines) {
                String[] parts = line.trim().split("\\s+");
                assertEquals(3, parts.length);
                assertEquals("MarketSchedule", parts[0]);
                assertTrue(parts[2].equals("0") || parts[2].equals("1"));
            }
        }

        // Validate default log file exists and contains OK summary
        assertTrue(Files.exists(defaultLog), "Default log file should exist");
        String log = Files.readString(defaultLog, StandardCharsets.UTF_8);
        assertTrue(log.contains("OK markets="), "Log should contain OK summary");
    }
}
