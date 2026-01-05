package com.dxfeed;

import com.dxfeed.schedule.Day;
import com.dxfeed.schedule.Schedule;
import com.dxfeed.schedule.Session;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;

public final class MarketScheduleUpdater {

    // ---- config ----
    private static final Duration GRACE = Duration.ofMinutes(5);
    private static final Path STATE_FILE = Paths.get("/var/lib/market-sd/state.properties"); // simple key=value
    // Default output and logs under project resources, convenient for local runs and CI
    private static final Path DEFAULT_OUT = Paths.get("src/main/resources/out/zabbix/zabbix_sender_input.txt");
    private static final Path DEFAULT_LOG = Paths.get("src/main/resources/out/logs/market_schedule.log");

    // Zabbix sender format: "<host> <itemKey> <value>"
    // zabbix_sender -z <server> -p <port> -i <file>
    private static final String ZABBIX_HOST = "MarketSchedule";

    public static void main(String[] args) {
        // args example:
        //   --defaults /etc/market-sd/dxfeed.schedule
        //   --markets  /etc/market-sd/markets.list   (lines: ID tv.KEY itemKey)
        //   --out      /run/market-sd/zabbix_sender_input.txt
        try {
            Map<String, String> cli = parseArgs(args);

            // Friendly behavior for Gradle run without args: print usage and exit 0
            if (args == null || args.length == 0) {
                printUsage();
                return;
            }

            // --defaults is now optional. If omitted, we will load dxFeed's built-in
            // schedule.properties from the dxfeed-api jar classpath.
            Path marketsPath  = Paths.get(required(cli, "--markets"));
            // Default local path to project resources
            Path outPath      = Paths.get(cli.getOrDefault("--out", DEFAULT_OUT.toString()));
            Path logPath      = Paths.get(cli.getOrDefault("--log", DEFAULT_LOG.toString()));

            // Load defaults from either --defaults or classpath; if external fails, fall back to classpath
            boolean defaultsSet = false;
            Path usedDefaultsPath = null;
            if (cli.containsKey("--defaults")) {
                try {
                    Path defaultsPath = Paths.get(required(cli, "--defaults"));
                    byte[] defaultsBytes = Files.readAllBytes(defaultsPath);
                    Schedule.setDefaults(defaultsBytes);
                    defaultsSet = true;
                    usedDefaultsPath = defaultsPath;
                } catch (Exception e) {
                    log("WARN failed to load external defaults, falling back to classpath: " + e);
                }
            }
            if (!defaultsSet) {
                try (InputStream in = MarketScheduleUpdater.class.getClassLoader()
                        .getResourceAsStream("com/dxfeed/schedule/schedule.properties")) {
                    if (in == null) {
                        throw new FileNotFoundException("Classpath resource com/dxfeed/schedule/schedule.properties not found");
                    }
                    Schedule.setDefaults(in.readAllBytes());
                }
            }

            List<MarketCfg> markets = loadMarkets(marketsPath);

            // Load aggregated schedule dataset (schedule_unique.csv) from resources
            Map<String, String> csvSchedules = loadAggregatedSchedules();

            // Load fallback tokens from schedule.properties (external defaults if provided, otherwise classpath)
            Map<String, String> propTokens = loadSchedulePropertiesTokens(usedDefaultsPath);

            Map<String, Boolean> prev = loadPrevState(STATE_FILE);

            Instant now = Instant.now();

            // Deduplicate schedules by definition to avoid recompute (optional but nice)
            Map<String, Schedule> scheduleCache = new HashMap<>();
            List<String> zabbixLines = new ArrayList<>();

            boolean anyStateChanged = false;

            for (MarketCfg m : markets) {
                Boolean prevState = prev.get(m.id);
                boolean computed;

                String csvToken = csvSchedules.get(m.tvKey);
                if (csvToken != null) {
                    // Prefer aggregated CSV token when available — no dxFeed lookup needed
                    computed = computeStateFromCsvToken(csvToken, now, prevState);
                } else {
                    String propToken = propTokens.get(m.tvKey);
                    if (propToken != null && !propToken.isBlank()) {
                        computed = computeStateFromCsvToken(propToken, now, prevState);
                        log("INFO using schedule.properties token for market=" + m.id + " tv=" + m.tvKey + " token=" + propToken);
                    } else {
                        // As last resort, try dxFeed resolution; if fails, use previous effective
                        try {
                            Schedule sch = Schedule.getInstance(m.tvKey);
                            computed = computeState(sch, now, prevState);
                            log("INFO used dxFeed schedule for market=" + m.id + " tv=" + m.tvKey);
                        } catch (Exception e) {
                            boolean prevEffective = (prevState != null) ? prevState : false;
                            computed = prevEffective;
                            log("WARN market=" + m.id + " tv=" + m.tvKey + " not found in CSV or schedule.properties; dxFeed failed: " + e + "; using fallback state=" + computed);
                        }
                    }
                }

                boolean prevEffective = (prevState != null) ? prevState : false; // default CLOSED
                if (computed != prevEffective) {
                    anyStateChanged = true;
                    prev.put(m.id, computed);
                }

                int value01 = computed ? 1 : 0;
                zabbixLines.add(ZABBIX_HOST + " " + m.itemKey + " " + value01);
            }

            // Write sender input always (idempotent), but only persist prev if changed
            Files.createDirectories(outPath.getParent());
            Files.write(outPath, zabbixLines, StandardCharsets.UTF_8);

            // Also produce per-market, per-day, per-minute files alongside the --out path
            try {
                generatePerMinuteFiles(outPath, markets, scheduleCache, csvSchedules, propTokens);
            } catch (Exception e) {
                log("WARN per-minute file generation failed: " + e);
            }

            if (anyStateChanged) {
                savePrevState(STATE_FILE, prev);
            }

            // Summary to stdout and file log
            String summary = "OK markets=" + markets.size() + " lines=" + zabbixLines.size() + " stateChanged=" + anyStateChanged;
            System.out.println(Instant.now() + " " + summary);
            logTo(logPath, summary);

        } catch (IllegalArgumentException e) {
            // Print usage only for missing required args
            if (e.getMessage() != null && e.getMessage().startsWith("Missing ")) {
                printUsage();
                return;
            } else {
                log("FATAL IllegalArgumentException: " + e);
                System.exit(2);
            }
        } catch (Exception e) {
            log("FATAL " + e);
            System.exit(2);
        }
    }

    // Small adapter interface to make unit testing easy without mocking dxFeed final classes
    interface ISchedule {
        Day getDayByTime(long millis);
        Session getSessionByTime(long millis);
    }

    static boolean computeState(Schedule schedule, Instant now, Boolean prevState) {
        // Delegate to interface-based implementation to simplify testing
        return computeState(new ISchedule() {
            @Override
            public Day getDayByTime(long millis) {
                return schedule.getDayByTime(millis);
            }

            @Override
            public Session getSessionByTime(long millis) {
                return schedule.getSessionByTime(millis);
            }
        }, now, prevState);
    }

    // Test-friendly core implementation
    static boolean computeState(ISchedule schedule, Instant now, Boolean prevState) {
        long tNow = now.toEpochMilli();

        Day dayNow = schedule.getDayByTime(tNow);
        if (!dayNow.isTrading()) { // holiday / non-trading day :contentReference[oaicite:5]{index=5}
            return false;
        }

        long tPlus = now.plus(GRACE).toEpochMilli();
        long tMinus = now.minus(GRACE).toEpochMilli();

        Session sPlus = schedule.getSessionByTime(tPlus);
        if (sPlus.isTrading()) { // within trading hours :contentReference[oaicite:6]{index=6}
            return true;
        }

        Session sMinus = schedule.getSessionByTime(tMinus);
        if (!sMinus.isTrading()) {
            return false;
        }

        // edge buffer
        return prevState != null ? prevState : false;
    }

    // ---- CSV-based state computation (using schedule_unique.csv tokens) ----
    static boolean computeStateFromCsvToken(String token, Instant now, Boolean prevState) {
        List<TimeInterval> intervals = parseCsvToken(token);
        // Safe-fail: if token cannot be parsed, do not change state
        if (intervals.isEmpty()) return prevState != null ? prevState : false;

        Instant tPlus = now.plus(GRACE);
        Instant tMinus = now.minus(GRACE);
        if (isTradingCsvAt(intervals, tPlus)) return true;
        if (!isTradingCsvAt(intervals, tMinus)) return false;
        return prevState != null ? prevState : false;
    }

    private static boolean isTradingCsvAt(List<TimeInterval> intervals, Instant t) {
        LocalTime time = t.atZone(ZoneOffset.UTC).toLocalTime();
        for (TimeInterval iv : intervals) {
            if (iv.allDay) return true;
            if (iv.overnight) {
                if (!iv.start.equals(iv.end)) {
                    if (!time.isBefore(iv.start) || time.isBefore(iv.end)) return true; // time >= start OR time < end
                } else {
                    return true; // start==end and overnight flag -> treat as 24h
                }
            } else {
                if ((time.equals(iv.start) || time.isAfter(iv.start)) && time.isBefore(iv.end)) return true;
            }
        }
        return false;
    }

    private static List<TimeInterval> parseCsvToken(String token) {
        List<TimeInterval> out = new ArrayList<>();
        if (token == null) return out;
        token = token.trim();
        if (token.isEmpty()) return out;
        if ("0000+0000".equals(token)) { // special always-open token from CSV
            out.add(TimeInterval.allDay());
            return out;
        }

        int i = 0; int n = token.length();
        while (i < n) {
            char c = token.charAt(i);
            if (c == 'p' || c == 'r' || c == 'a') {
                if (i + 9 <= n && isDigits(token, i + 1, 8)) {
                    String s = token.substring(i + 1, i + 5);
                    String e = token.substring(i + 5, i + 9);
                    addInterval(out, s, e);
                    i += 9;
                    continue;
                } else {
                    i++;
                    continue;
                }
            } else if (c == '-') { // overnight window expressed explicitly
                if (i + 9 <= n && isDigits(token, i + 1, 8)) {
                    String s = token.substring(i + 1, i + 5);
                    String e = token.substring(i + 5, i + 9);
                    addIntervalOvernight(out, s, e);
                    i += 9;
                    continue;
                } else {
                    i++;
                    continue;
                }
            } else if (Character.isDigit(c)) {
                int j = i;
                while (j < n && Character.isDigit(token.charAt(j))) j++;
                String digits = token.substring(i, j);
                for (int k = 0; k + 8 <= digits.length(); k += 8) {
                    String s = digits.substring(k, k + 4);
                    String e = digits.substring(k + 4, k + 8);
                    addInterval(out, s, e);
                }
                i = j;
                continue;
            } else {
                i++;
            }
        }
        return out;
    }

    private static boolean isDigits(String s, int off, int len) {
        if (off + len > s.length()) return false;
        for (int i = off; i < off + len; i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private static void addInterval(List<TimeInterval> out, String hhmmStart, String hhmmEnd) {
        LocalTime start = parseHHmm(hhmmStart);
        LocalTime end = parseHHmm(hhmmEnd);
        boolean overnight = end.isBefore(start) || end.equals(LocalTime.MIDNIGHT) && start.equals(LocalTime.MIDNIGHT);
        if (overnight && start.equals(end)) {
            out.add(TimeInterval.allDay());
        } else if (overnight) {
            out.add(new TimeInterval(start, end, true, false));
        } else {
            out.add(new TimeInterval(start, end, false, false));
        }
    }

    private static void addIntervalOvernight(List<TimeInterval> out, String hhmmStart, String hhmmEnd) {
        LocalTime start = parseHHmm(hhmmStart);
        LocalTime end = parseHHmm(hhmmEnd);
        if (start.equals(end)) {
            out.add(TimeInterval.allDay());
        } else {
            out.add(new TimeInterval(start, end, true, false));
        }
    }

    private static LocalTime parseHHmm(String hhmm) {
        int hh = Integer.parseInt(hhmm.substring(0, 2));
        int mm = Integer.parseInt(hhmm.substring(2, 4));
        return LocalTime.of(hh % 24, mm % 60);
    }

    private static final class TimeInterval {
        final LocalTime start;
        final LocalTime end;
        final boolean overnight;
        final boolean allDay;

        TimeInterval(LocalTime start, LocalTime end, boolean overnight, boolean allDay) {
            this.start = start; this.end = end; this.overnight = overnight; this.allDay = allDay;
        }
        static TimeInterval allDay() { return new TimeInterval(LocalTime.MIDNIGHT, LocalTime.MIDNIGHT, true, true); }
    }

    // Schedule.getInstance("tv.*") resolves the right definition.

    // ---- markets list loader ----
    // Each line: <id> <tvKey> <itemKey>
    // Example: CME_GLBX tv.GLBX CME_market_state
    private static List<MarketCfg> loadMarkets(Path p) throws IOException {
        List<MarketCfg> out = new ArrayList<>();
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("#")) continue;
            String[] parts = t.split("\\s+");
            if (parts.length < 3) {
                log("WARN bad markets line, skipping: " + line);
                continue;
            }
            out.add(new MarketCfg(parts[0], parts[1], parts[2]));
        }
        return out;
    }

    private static Map<String, Boolean> loadPrevState(Path p) {
        Map<String, Boolean> out = new HashMap<>();
        if (!Files.exists(p)) return out;
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(p)) {
            props.load(in);
            for (String name : props.stringPropertyNames()) {
                out.put(name, "1".equals(props.getProperty(name)) || "true".equalsIgnoreCase(props.getProperty(name)));
            }
        } catch (Exception e) {
            log("WARN cannot read prev state: " + e);
        }
        return out;
    }

    private static void savePrevState(Path p, Map<String, Boolean> state) {
        try {
            Files.createDirectories(p.getParent());
            Properties props = new Properties();
            for (var e : state.entrySet()) {
                props.setProperty(e.getKey(), e.getValue() ? "1" : "0");
            }
            try (OutputStream out = Files.newOutputStream(p, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                props.store(out, "market state");
            }
        } catch (Exception e) {
            log("ERROR cannot persist prev state: " + e);
        }
    }

    // ---- tiny CLI args helper ----
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                String v = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true";
                m.put(a, v);
            }
        }
        return m;
    }

    private static String required(Map<String, String> m, String k) {
        String v = m.get(k);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing " + k);
        return v;
    }

    private static void log(String s) {
        System.out.println(Instant.now() + " " + s);
        // Fallback simple file logging to default log file
        logTo(DEFAULT_LOG, s);
    }

    private static void logTo(Path logFile, String s) {
        String line = Instant.now() + " " + s + System.lineSeparator();
        try {
            if (logFile != null) {
                Files.createDirectories(logFile.getParent());
                Files.writeString(logFile, line, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (Exception ignore) {
            // ignore file logging errors
        }
    }

    // ---- per-minute per-day files ----
    private static void generatePerMinuteFiles(Path outPath, List<MarketCfg> markets, Map<String, Schedule> scheduleCache, Map<String, String> csvSchedules) throws IOException {
        generatePerMinuteFiles(outPath, markets, scheduleCache, csvSchedules, Collections.emptyMap());
    }

    // Overload with schedule.properties tokens support
    private static void generatePerMinuteFiles(Path outPath, List<MarketCfg> markets, Map<String, Schedule> scheduleCache, Map<String, String> csvSchedules, Map<String, String> propTokens) throws IOException {
        Path dir = outPath.getParent();
        String base = baseName(outPath.getFileName().toString());
        // Use UTC day for file naming
        LocalDate todayUtc = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();
        String yyyymmdd = String.format(java.util.Locale.ROOT, "%04d%02d%02d", todayUtc.getYear(), todayUtc.getMonthValue(), todayUtc.getDayOfMonth());

        for (MarketCfg m : markets) {
            Path f = dir.resolve(base + "_" + m.id + "_" + yyyymmdd + ".txt");
            Files.createDirectories(f.getParent());
            List<String> lines = new ArrayList<>(24 * 60);
            // Minute-by-minute from 00:00:00 to 23:59:00 UTC
            Instant t = todayUtc.atStartOfDay().toInstant(ZoneOffset.UTC);
            Boolean prev = null;

            Schedule schedule = null;
            Exception scheduleErr = null;
            String csvToken = csvSchedules != null ? csvSchedules.get(m.tvKey) : null;
            String propToken = (propTokens != null) ? propTokens.get(m.tvKey) : null;
            if (csvToken == null && propToken == null) {
                // No CSV token — try dxFeed schedule resolution
                try {
                    schedule = scheduleCache.computeIfAbsent(m.tvKey, Schedule::getInstance);
                } catch (Exception e) {
                    scheduleErr = e;
                    log("ERROR per-minute schedule resolve failed market=" + m.id + " tv=" + m.tvKey + " err=" + e + "; writing zeros");
                }
            } else if (csvToken != null) {
                log("INFO per-minute using CSV token for market=" + m.id + " tv=" + m.tvKey + " schedule=" + csvToken);
            } else if (propToken != null) {
                log("INFO per-minute using schedule.properties token for market=" + m.id + " tv=" + m.tvKey + " schedule=" + propToken);
            }

            for (int i = 0; i < 24 * 60; i++) {
                boolean state;
                if (csvToken != null) {
                    // Compute from CSV definition
                    try {
                        state = computeStateFromCsvToken(csvToken, t, prev);
                    } catch (Exception ex) {
                        state = prev != null ? prev : false;
                        log("WARN CSV computeState failed market=" + m.id + " tv=" + m.tvKey + " minute=" + t + " err=" + ex + "; using prev/fallback");
                    }
                } else if (propToken != null) {
                    try {
                        state = computeStateFromCsvToken(propToken, t, prev);
                    } catch (Exception ex) {
                        state = prev != null ? prev : false;
                        log("WARN PROPS computeState failed market=" + m.id + " tv=" + m.tvKey + " minute=" + t + " err=" + ex + "; using prev/fallback");
                    }
                } else if (schedule == null) {
                    state = false;
                } else {
                    try {
                        state = computeState(schedule, t, prev);
                    } catch (Exception ex) {
                        // If any computation error occurs, fall back to previous or 0
                        state = prev != null ? prev : false;
                        if (scheduleErr == null) {
                            log("WARN computeState failed market=" + m.id + " tv=" + m.tvKey + " minute=" + t + " err=" + ex);
                        }
                    }
                }
                prev = state;
                String timeText = t.atZone(ZoneOffset.UTC).toLocalTime().toString(); // HH:mm or HH:mm:ss
                if (timeText.length() == 5) { // ensure HH:mm:ss
                    timeText = timeText + ":00";
                }
                long epoch = t.getEpochSecond();
                lines.add(timeText + " " + epoch + " " + (state ? 1 : 0));
                t = t.plus(Duration.ofMinutes(1));
            }
            Files.write(f, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log("INFO wrote per-minute file file=" + f + " lines=" + lines.size());
        }
    }

    private static String baseName(String fileName) {
        int dot = fileName.lastIndexOf('.');
        if (dot > 0) return fileName.substring(0, dot);
        return fileName;
    }

    // ---- aggregated schedules loader (schedule_unique.csv) ----
    // Made package-private to be reusable by GoldenVectorGenerator
    static Map<String, String> loadAggregatedSchedules() {
        Map<String, String> map = new HashMap<>();
        try (InputStream in = MarketScheduleUpdater.class.getClassLoader().getResourceAsStream("schedule_unique.csv")) {
            if (in == null) {
                log("WARN schedule_unique.csv not found on classpath; proceeding without CSV validation");
                return map;
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                boolean header = true;
                while ((line = br.readLine()) != null) {
                    if (header) { header = false; continue; }
                    String[] parts = line.split(",", -1);
                    if (parts.length < 4) continue;
                    String scheduleToken = parts[0].trim();
                    String tvAll = parts[3].trim();
                    if (scheduleToken.isEmpty() || tvAll.isEmpty()) continue;
                    // tv_all entries are separated by ';' and may have optional spaces
                    String[] tvs = tvAll.split(";\\s*");
                    for (String tv : tvs) {
                        String key = tv.trim();
                        if (key.isEmpty()) continue;
                        int at = key.indexOf('@');
                        if (at > 0) key = key.substring(0, at);
                        map.putIfAbsent(key, scheduleToken);
                    }
                }
            }
            log("INFO loaded aggregated schedules from CSV: tv_keys=" + map.size());
        } catch (Exception e) {
            log("WARN failed to load schedule_unique.csv: " + e);
        }
        return map;
    }

    // ---- schedule.properties fallback loader ----
    // Reads schedule definitions for tv.* keys from either an external defaults file (if provided)
    // or from classpath resource com/dxfeed/schedule/schedule.properties. Extracts time window tokens
    // compatible with computeStateFromCsvToken (e.g., p04000930r09301600a16002000, -17001615, 09301600, 0000+0000).
    // Made package-private to be reusable by GoldenVectorGenerator
    static Map<String, String> loadSchedulePropertiesTokens(Path defaultsPath) {
        Map<String, String> map = new HashMap<>();
        try {
            List<String> lines;
            if (defaultsPath != null) {
                try {
                    lines = Files.readAllLines(defaultsPath, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    lines = null;
                }
            } else {
                // Try conventional conf/dxfeed.schedule first if present in working directory
                Path confPath = Paths.get("conf/dxfeed.schedule");
                if (Files.exists(confPath)) {
                    try {
                        lines = Files.readAllLines(confPath, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        lines = null;
                    }
                } else {
                    lines = null;
                }
            }
            if (lines == null) {
                try (InputStream in = MarketScheduleUpdater.class.getClassLoader()
                        .getResourceAsStream("com/dxfeed/schedule/schedule.properties")) {
                    if (in != null) {
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                processSchedulePropsLine(map, line);
                            }
                        }
                    }
                }
            } else {
                for (String line : lines) {
                    processSchedulePropsLine(map, line);
                }
            }
            if (!map.isEmpty()) {
                log("INFO loaded schedule.properties tokens: tv_keys=" + map.size());
            }
        } catch (Exception e) {
            log("WARN failed to load schedule.properties tokens: " + e);
        }
        return map;
    }

    private static void processSchedulePropsLine(Map<String, String> map, String rawLine) {
        if (rawLine == null) return;
        String line = rawLine.trim();
        if (line.isEmpty() || line.startsWith("#")) return;
        int eq = line.indexOf('=');
        if (eq <= 0) return;
        String key = line.substring(0, eq).trim();
        if (!key.startsWith("tv.")) return;
        String rhs = line.substring(eq + 1);
        String token = extractTokenFromScheduleDefinition(rhs);
        if (token != null && !token.isBlank()) {
            map.putIfAbsent(key, token);
        }
    }

    // Extracts a token string composed of one or more windows in forms we support:
    // - pHHMMHHMM, rHHMMHHMM, aHHMMHHMM
    // - -HHMMHHMM (overnight)
    // - concatenated plain HHMMHHMM pairs (multiple pairs are concatenated)
    // - special 0000+0000 (always open)
    // Prefers explicit "0=" assignments when present (e.g., 0=-17001615), otherwise scans the RHS.
    private static String extractTokenFromScheduleDefinition(String rhs) {
        if (rhs == null) return null;
        String s = rhs.trim();
        if (s.isEmpty()) return null;

        // If the definition contains an explicit 0= assignment, use it
        int idx0 = s.indexOf("0=");
        if (idx0 >= 0) {
            int start = idx0 + 2;
            int end = s.indexOf(';', start);
            String t = (end >= 0) ? s.substring(start, end) : s.substring(start);
            t = t.trim();
            // keep only allowed token chars
            t = t.replaceAll("[^0-9pra\\-+]+", "");
            if (!t.isBlank()) return t;
        }

        // Special always-open marker, keep literal (parseCsvToken knows it)
        if (s.contains("0000+0000")) return "0000+0000";

        StringBuilder out = new StringBuilder();
        int n = s.length();
        int i = 0;
        while (i < n) {
            char c = s.charAt(i);
            if (c == 'p' || c == 'r' || c == 'a') {
                if (i + 9 <= n && isDigits(s, i + 1, 8)) {
                    out.append(s, i, i + 9);
                    i += 9;
                    continue;
                } else { i++; continue; }
            } else if (c == '-') {
                if (i + 9 <= n && isDigits(s, i + 1, 8)) {
                    out.append(s, i, i + 9);
                    i += 9;
                    continue;
                } else { i++; continue; }
            } else if (Character.isDigit(c)) {
                int j = i;
                while (j < n && Character.isDigit(s.charAt(j))) j++;
                String digits = s.substring(i, j);
                for (int k = 0; k + 8 <= digits.length(); k += 8) {
                    out.append(digits, k, k + 8);
                }
                i = j;
                continue;
            } else {
                i++;
            }
        }
        String token = out.toString();
        return token.isBlank() ? null : token;
    }


    private static void printUsage() {
        System.out.println("Usage: java -jar <jar> [--defaults <path to dxfeed.schedule>] --markets <path to markets.list> [--out <path to zabbix sender input>] [--log <path to log file>]");
        System.out.println("Notes: If --defaults is omitted, the tool loads schedule.properties from the dxfeed-api classpath.");
        System.out.println("Default out: " + DEFAULT_OUT);
        System.out.println("Default log: " + DEFAULT_LOG);
        System.out.println("Example (classpath defaults): --markets conf/markets.list --out src/main/resources/out/zabbix/zabbix_sender_input.txt");
        System.out.println("Example (external defaults): --defaults conf/dxfeed.schedule --markets conf/markets.list --out src/main/resources/out/zabbix/zabbix_sender_input.txt --log src/main/resources/out/logs/market_schedule.log");
    }

    private record MarketCfg(String id, String tvKey, String itemKey) {}
}
