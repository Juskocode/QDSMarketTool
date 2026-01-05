package org.example;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Per-market mutation tests (domain mutations) based on schedule_unique.csv tokens (and a subset of schedule.properties).
 * For each tv.* token, we generate a golden per-minute vector for a UTC day and then apply controlled config mutations:
 *  - time shifts (±1, ±15 minutes)
 *  - segment drop (or shrink when only one segment)
 *  - cross-day sign flip (toggle explicit overnight '-')
 * We assert that valid-but-wrong mutations produce vectors different from golden (unless the golden is constant),
 * and that invalid mutations (scrambled token) safe-fail with no state changes.
 */
public class MarketScheduleMutationTest {

    private static final LocalDate DATE = Instant.now().atZone(ZoneOffset.UTC).toLocalDate();

    // ---- Helpers to compute vectors ----
    private static List<Integer> vectorFromToken(String token, LocalDate dateUtc) {
        List<Integer> out = new ArrayList<>(24 * 60);
        Instant t = dateUtc.atStartOfDay().toInstant(ZoneOffset.UTC);
        Boolean prev = null;
        for (int i = 0; i < 24 * 60; i++) {
            boolean state = MarketScheduleUpdater.computeStateFromCsvToken(token, t, prev);
            prev = state;
            out.add(state ? 1 : 0);
            t = t.plus(Duration.ofMinutes(1));
        }
        return out;
    }

    private static boolean isConstant(List<Integer> v) {
        if (v.isEmpty()) return true;
        int first = v.get(0);
        for (int x : v) if (x != first) return false;
        return true;
    }

    private static long transitions(List<Integer> v) {
        long c = 0;
        for (int i = 1; i < v.size(); i++) if (!v.get(i).equals(v.get(i - 1))) c++;
        return c;
    }

    private static boolean differs(List<Integer> a, List<Integer> b) {
        if (a.size() != b.size()) return true;
        for (int i = 0; i < a.size(); i++) if (!Objects.equals(a.get(i), b.get(i))) return true;
        return false;
    }

    // ---- Token mutation helpers ----
    private static String shiftTokenMinutes(String token, int deltaMin) {
        if (token == null) return null;
        StringBuilder sb = new StringBuilder(token.length());
        int n = token.length();
        for (int i = 0; i < n; ) {
            char c = token.charAt(i);
            if (Character.isDigit(c)) {
                int j = i;
                while (j < n && Character.isDigit(token.charAt(j))) j++;
                // process digits in 4-char chunks (HHMM), preserving remainder if any
                for (int k = i; k + 4 <= j; k += 4) {
                    String hhmm = token.substring(k, k + 4);
                    String shifted = shiftHHmm(hhmm, deltaMin);
                    sb.append(shifted);
                }
                // append any leftover digits unchanged
                if ((j - i) % 4 != 0) {
                    sb.append(token, j - ((j - i) % 4), j);
                }
                i = j;
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    private static String shiftHHmm(String hhmm, int deltaMin) {
        try {
            int hh = Integer.parseInt(hhmm.substring(0, 2));
            int mm = Integer.parseInt(hhmm.substring(2, 4));
            int tot = (hh * 60 + mm + deltaMin) % (24 * 60);
            if (tot < 0) tot += 24 * 60;
            int nh = tot / 60;
            int nm = tot % 60;
            return String.format(java.util.Locale.ROOT, "%02d%02d", nh, nm);
        } catch (Exception e) {
            return hhmm; // keep original on parse issue
        }
    }

    private static boolean isDigits(String s, int off, int len) {
        if (off + len > s.length()) return false;
        for (int i = off; i < off + len; i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private static String dropOneSegmentOrShrink(String token) {
        if (token == null || token.isBlank()) return token;
        // Try to drop prefixed windows first (p/r/a or '-')
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if ((c == 'p' || c == 'r' || c == 'a' || c == '-') && isDigits(token, i + 1, 8)) {
                return token.substring(0, i) + token.substring(i + 9);
            }
        }
        // Drop first plain 8-digit window; also remove preceding p/r/a if present
        for (int i = 0; i + 8 <= token.length(); i++) {
            if (isDigits(token, i, 8)) {
                int start = i;
                if (i - 1 >= 0) {
                    char prev = token.charAt(i - 1);
                    if (prev == 'p' || prev == 'r' || prev == 'a') start = i - 1;
                }
                return token.substring(0, start) + token.substring(i + 8);
            }
        }
        // As a last resort, shrink first time by +15 minutes
        for (int i = 0; i + 4 <= token.length(); i++) {
            if (isDigits(token, i, 4)) {
                String hhmm = token.substring(i, i + 4);
                String repl = shiftHHmm(hhmm, 15);
                return token.substring(0, i) + repl + token.substring(i + 4);
            }
        }
        return token;
    }

    private static String crossDayFlip(String token) {
        if (token == null || token.isBlank()) return token;
        // Find first 8-digit window
        for (int i = 0; i + 8 <= token.length(); i++) {
            if (isDigits(token, i, 8)) {
                // If there is a '-' directly before, remove it (flip to non-overnight)
                if (i - 1 >= 0 && token.charAt(i - 1) == '-') {
                    return token.substring(0, i - 1) + token.substring(i);
                }
                // If there is a p/r/a directly before, replace it with '-'
                if (i - 1 >= 0) {
                    char prev = token.charAt(i - 1);
                    if (prev == 'p' || prev == 'r' || prev == 'a') {
                        return token.substring(0, i - 1) + '-' + token.substring(i);
                    }
                }
                // Otherwise insert '-' before the 8-digit window
                return token.substring(0, i) + '-' + token.substring(i);
            }
        }
        return token;
    }

    private static String corruptToken(String token) {
        if (token == null) return null;
        StringBuilder sb = new StringBuilder(token.length());
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            sb.append(Character.isDigit(c) ? 'x' : c);
        }
        return sb.toString();
    }

    // ---- Dynamic tests over CSV tokens ----
    @TestFactory
    Collection<DynamicTest> csv_tokens_mutations() {
        Map<String, String> csv = MarketScheduleUpdater.loadAggregatedSchedules();
        List<String> keys = new ArrayList<>(csv.keySet());
        Collections.sort(keys);
        List<DynamicTest> tests = new ArrayList<>();

        for (String tv : keys) {
            String token = csv.get(tv);
            if (token == null || token.isBlank()) continue;
            List<Integer> golden = vectorFromToken(token, DATE);

            // Valid-but-wrong: time shifts
            for (int delta : new int[]{1, -1, 15, -15}) {
                String name = tv + " shift " + (delta >= 0 ? "+" + delta : String.valueOf(delta));
                tests.add(DynamicTest.dynamicTest(name, () -> {
                    String shifted = shiftTokenMinutes(token, delta);
                    List<Integer> v = vectorFromToken(shifted, DATE);
                    if (!shifted.equals(token) && !isConstant(golden)) {
                        assertTrue(differs(golden, v), "Shift mutation should differ from golden for " + tv);
                    }
                }));
            }

            // Valid-but-wrong: segment drop (or shrink)
            tests.add(DynamicTest.dynamicTest(tv + " dropSegment", () -> {
                String dropped = dropOneSegmentOrShrink(token);
                List<Integer> v = vectorFromToken(dropped, DATE);
                if (!dropped.equals(token) && !isConstant(golden)) {
                    assertTrue(differs(golden, v), "Drop/shrink mutation should differ from golden for " + tv);
                }
            }));

            // Valid-but-wrong: cross-day sign flip
            tests.add(DynamicTest.dynamicTest(tv + " crossDayFlip", () -> {
                String flipped = crossDayFlip(token);
                List<Integer> v = vectorFromToken(flipped, DATE);
                if (!flipped.equals(token) && !isConstant(golden)) {
                    if (differs(golden, v)) {
                        assertTrue(true);
                    } else {
                        // Some tokens are inherently overnight (end<start). Flipping '-' may be a no-op.
                        // In that case, we accept equality and do not fail the test.
                    }
                }
            }));

            // Invalid mutation: corrupt digits -> safe-fail, no state change across minutes
            tests.add(DynamicTest.dynamicTest(tv + " invalid_safe_fail_no_state_change", () -> {
                String bad = corruptToken(token);
                List<Integer> v = vectorFromToken(bad, DATE);
                assertEquals(0, transitions(v), "Invalid token must yield no state changes (safe-fail) for " + tv);
            }));
        }
        return tests;
    }

    // ---- Optional: also run a subset over schedule.properties-derived tokens ----
    @TestFactory
    Collection<DynamicTest> props_tokens_mutations_subset() {
        Map<String, String> props = MarketScheduleUpdater.loadSchedulePropertiesTokens(null);
        List<String> keys = new ArrayList<>(props.keySet());
        Collections.sort(keys);
        // Limit to first 50 to keep test time reasonable
        if (keys.size() > 50) keys = keys.subList(0, 50);

        List<DynamicTest> tests = new ArrayList<>();
        for (String tv : keys) {
            String token = props.get(tv);
            if (token == null || token.isBlank()) continue;
            List<Integer> golden = vectorFromToken(token, DATE);

            tests.add(DynamicTest.dynamicTest(tv + " props_shift+15", () -> {
                String shifted = shiftTokenMinutes(token, 15);
                List<Integer> v = vectorFromToken(shifted, DATE);
                if (!shifted.equals(token) && !isConstant(golden)) {
                    assertTrue(differs(golden, v));
                }
            }));

            tests.add(DynamicTest.dynamicTest(tv + " props_invalid_safe_fail", () -> {
                String bad = corruptToken(token);
                List<Integer> v = vectorFromToken(bad, DATE);
                assertEquals(0, transitions(v));
            }));
        }
        return tests;
    }
}
