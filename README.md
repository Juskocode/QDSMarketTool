QDSMarketTool

Overview
- A small CLI tool that determines per-market OPEN/CLOSED state using the dxFeed Schedule API. It reads market allowlist entries, evaluates the current schedule with a 5‑minute edge buffer, and writes Zabbix sender input lines.

Key features
- Uses dxFeed schedule semantics (holidays, short days, day-end alignment) via Schedule.setDefaults.
- Resolves schedules by key (e.g., tv.GLBX) using Schedule.getInstance.
- Implements the edge-buffer algorithm:
  - If today is not a trading day → CLOSED
  - Else if session at now+5m is trading → OPEN
  - Else if session at now−5m is not trading → CLOSED
  - Else → keep previous state
- Persists previous state to /var/lib/market-sd/state.properties.
- Emits Zabbix sender lines: "MarketSchedule <itemKey> <0|1>".

Golden vectors (reference per-minute outputs)
- A helper utility GoldenVectorGenerator is included to generate per‑minute golden vectors (0/1) for each tv.* key
  based on two sources:
  - schedule_unique.csv (aggregated schedule tokens) → writes to .../golden/csv
  - schedule.properties (from dxfeed defaults or an external dxfeed.schedule) → writes to .../golden/props
  Each file contains 1440 lines for a UTC day in the format: "HH:mm:ss <epochSeconds> <0|1>".
  Usage examples:
  - ./gradlew runGolden --args="--outDir src/main/resources/out/zabbix/golden --date 2026-01-05"
  - ./gradlew runGolden --args="--defaults conf/dxfeed.schedule --outDir src/main/resources/out/zabbix/golden --date 2026-01-05"
  Output example: src/main/resources/out/zabbix/golden/csv/golden_csv_XETR_20260105.txt

Prerequisites
- JDK 17 installed
- Gradle Wrapper (included)
- Place required jars into libs/ directory:
  - libs/dxfeed-api-3.348.jar
  - libs/qds-3.348.jar
  - If your build needs additional Devexperts IO classes (com.devexperts.io.*, e.g., ByteArrayInput), drop that jar into libs/ as well. Gradle is configured to include all jars from libs/ automatically.

Project layout (selected)
- build.gradle.kts — Gradle build (Application + Shadow for fat jar)
- libs/dxfeed-api-3.348.jar — dxFeed API (contains com/dxfeed/schedule/* and schedule.properties)
- src/main/java/org/example/MarketScheduleUpdater.java — main CLI tool
- src/test/java/org/example/MarketScheduleUpdaterTest.java — JUnit 5 + Mockito tests
- conf/markets.list — sample markets allowlist
- conf/dxfeed.schedule — placeholder (optional; you can supply a real defaults file)
- qds_unique.py — helper to analyze schedule.properties for unique schedules/sources

Quick start (local run)
1) Verify the dxFeed JAR exists
   - Check libs/dxfeed-api-3.348.jar is present.

2) Prepare configs
   - markets.list (example provided in conf/markets.list):
     CME_GLBX    tv.GLBX      CME_market_state
     CME_XCME    tv.XCME      CME_floor_market_state
     CME_CLR     tv.ClearPort CME_clearport_market_state

3) Run using built-in schedule.properties from the dxFeed JAR
   - The dxfeed-api JAR contains com/dxfeed/schedule/schedule.properties which will be used by default.
   - Command (writes output + logs under src/main/resources/out/... by default):
     ./gradlew run --args="--markets conf/markets.list"
   - After running, you will find:
     - Zabbix sender file: src/main/resources/out/zabbix/zabbix_sender_input.txt
     - Logs: src/main/resources/out/logs/market_schedule.log

4) (Optional) Run with an external dxfeed.schedule file
   - If you want to supply an external defaults file instead of the built-in schedule.properties:
     ./gradlew run --args="--defaults conf/dxfeed.schedule --markets conf/markets.list --out src/main/resources/out/zabbix/zabbix_sender_input.txt --log src/main/resources/out/logs/market_schedule.log"

Output
- The tool writes Zabbix sender input lines to the path passed with --out (default for local runs: src/main/resources/out/zabbix/zabbix_sender_input.txt):
  MarketSchedule CME_market_state 1
  MarketSchedule CME_floor_market_state 0

Build
- Build (regular):
  ./gradlew build

- Build a fat jar (self-contained with dependencies) using Shadow plugin:
  ./gradlew clean shadowJar
  The jar will be at build/libs/QDSMarketTool-1.0-SNAPSHOT.jar

Run the fat jar directly
- Using built-in schedule.properties (classpath inside dxfeed-api JAR):
  java -jar build/libs/QDSMarketTool-1.0-SNAPSHOT.jar \
    --markets conf/markets.list \
    --out src/main/resources/out/zabbix/zabbix_sender_input.txt \
    --log src/main/resources/out/logs/market_schedule.log

Generate golden vectors via Gradle
- Run the dedicated task with optional args:
-  ./gradlew runGolden --args="--outDir src/main/resources/out/zabbix/golden --date 2026-01-05"
-  ./gradlew runGolden --args="--defaults conf/dxfeed.schedule --outDir src/main/resources/out/zabbix/golden --date 2026-01-05"

- Using an external dxfeed.schedule file:
  java -jar build/libs/QDSMarketTool-1.0-SNAPSHOT.jar \
    --defaults /path/to/dxfeed.schedule \
    --markets conf/markets.list \
    --out /tmp/zabbix_sender_input.txt

CLI flags
- --markets <path> — REQUIRED. Path to allowlist file; each non-comment line: "<id> <tvKey> <itemKey>".
- --out <path> — Optional. Output file for Zabbix sender input. Default: src/main/resources/out/zabbix/zabbix_sender_input.txt.
- --log <path> — Optional. Log file path. Default: src/main/resources/out/logs/market_schedule.log.
- --defaults <path> — Optional. External dxfeed.schedule defaults. If omitted, the app loads com/dxfeed/schedule/schedule.properties from the classpath (dxfeed-api JAR).

markets.list format
- Columns are whitespace-separated (multiple spaces or tabs are fine):
  - id: Arbitrary identifier used to persist previous state (e.g., CME_GLBX)
  - tvKey: Schedule key to evaluate (e.g., tv.GLBX)
  - itemKey: Zabbix item key to publish (e.g., CME_market_state)

Algorithm details
- Grace window: 5 minutes.
- State persistence: /var/lib/market-sd/state.properties (created automatically). Values: key=<id>, value 0/1.
- Failure rule: on schedule parsing or lookup exception, the tool logs an error and does not change previous state.

Tests
- Run all tests:
  ./gradlew test

- Run a specific test class (from IDE or using gradle’s test filtering if desired).

What’s covered by tests
- Non-trading day → CLOSED
- now+5m trading → OPEN
- now−5m not trading → CLOSED
- Edge buffer retains previous state (or defaults to CLOSED when previous is null)

Deployment (systemd + zabbix_sender)
1) Create directories on the host
   sudo mkdir -p /opt/market-sd /etc/market-sd /var/lib/market-sd /run/market-sd

2) Copy artifacts
   sudo cp build/libs/QDSMarketTool-1.0-SNAPSHOT.jar /opt/market-sd/market-sd.jar
   sudo cp conf/markets.list /etc/market-sd/
   # Optional: external defaults (you can omit if using classpath schedule.properties)
   sudo cp conf/dxfeed.schedule /etc/market-sd/

3) Create service user
   sudo useradd --system --home /var/lib/market-sd --shell /usr/sbin/nologin market-sd || true
   sudo chown -R market-sd:market-sd /var/lib/market-sd /run/market-sd

4) Systemd unit: /etc/systemd/system/market-sd.service
   [Unit]
   Description=Market Schedule State Updater
   After=network-online.target

   [Service]
   Type=oneshot
   User=market-sd
   Group=market-sd
   ExecStart=/usr/bin/java -jar /opt/market-sd/market-sd.jar \
     --markets /etc/market-sd/markets.list \
     --out /run/market-sd/zabbix_sender_input.txt
   # Or, if you prefer an external defaults file:
   # ExecStart=/usr/bin/java -jar /opt/market-sd/market-sd.jar \
   #   --defaults /etc/market-sd/dxfeed.schedule \
   #   --markets /etc/market-sd/markets.list \
   #   --out /run/market-sd/zabbix_sender_input.txt

   ExecStartPost=/usr/bin/zabbix_sender -z <ZABBIX_SERVER> -p 10051 \
     -i /run/market-sd/zabbix_sender_input.txt

5) Timer unit: /etc/systemd/system/market-sd.timer
   [Unit]
   Description=Run Market Schedule Updater every minute

   [Timer]
   OnBootSec=30
   OnUnitActiveSec=60
   AccuracySec=1s

   [Install]
   WantedBy=timers.target

6) Enable
   sudo systemctl daemon-reload
   sudo systemctl enable --now market-sd.timer
   sudo systemctl status market-sd.timer
   journalctl -u market-sd.service -f

Troubleshooting
- LinkageError / NoClassDefFoundError (e.g., com/devexperts/io/ByteArrayInput)
  - Cause: missing Devexperts transitive classes required by dxFeed Schedule API.
  - Fix: place all required jars in libs/. Gradle picks up every *.jar in libs/ for both `run` and the Shadow fat jar. Minimum: dxfeed-api and qds. If your dxFeed build also requires Devexperts IO classes (package `com.devexperts.io.*`), add that jar too.
  - Use the fat jar: ./gradlew shadowJar, then run java -jar build/libs/QDSMarketTool-1.0-SNAPSHOT.jar
  - If you use ./gradlew run and a required runtime jar is missing, the tool will log a helpful hint and exit gracefully without failing the build. Prefer running the fat jar for an all-in-one classpath.

- UnsupportedClassVersionError
  - Ensure JDK 17 is used (project pins toolchain to 17).

- Permission denied writing /run or /var/lib
  - For local runs, use --out /tmp/... (default) and run without persisting state, or create writable directories.
  - In production, run under market-sd user and ensure ownership of /var/lib/market-sd and /run/market-sd.

- Schedule lookup by key fails (Schedule.getInstance("tv.GLBX"))
  - Verify that the defaults file is correctly loaded (either classpath schedule.properties or the external path).
  - Try another known key for your dataset; capture the exception text for diagnosis.

Python helper (qds_unique.py)
- Inspect unique schedules/sources from a schedule.properties-like file.
- Examples:
  python qds_unique.py schedule.properties --mode schedules
  python qds_unique.py schedule.properties --mode sources
  python qds_unique.py schedule.properties --mode schedules --full-fingerprint

Licensing and third-party notices
- This project integrates dxFeed scheduling data and libraries. Use of any dxFeed artifacts requires a valid license from Devexperts/dxFeed. You are responsible for ensuring your usage complies with your dxFeed agreement.
  - Bundled/required runtime artifacts (placed under libs/ by you):
    - dxfeed-api-<version>.jar (e.g., dxfeed-api-3.348.jar)
    - dxlib-<version>.jar (e.g., dxlib-3.348.jar)
    - qds-<version>.jar (e.g., qds-3.348.jar)
    These are proprietary components owned by Devexperts/dxFeed and are not distributed by this repository. Do not redistribute them unless your license permits it.
  - Defaults and schedules:
    - The tool can load schedule definitions from the dxFeed defaults (classpath resource com/dxfeed/schedule/schedule.properties) or from an external dxfeed.schedule file you provide via --defaults.
    - The included schedule_unique.csv is an aggregated reference of schedules intended for testing and development. It may reflect information derived from dxFeed defaults. Treat it as reference-only and do not redistribute it beyond the terms of your dxFeed license.
  - Market data and venue calendars may be subject to exchange/vendor licensing in addition to dxFeed terms.

  - dxFeed legal terms and licensing:
    - dxFeed legal overview: https://dxfeed.com/legal
    - Contact dxFeed for licensing questions: https://dxfeed.com/contact/

- Trademarks: dxFeed and Devexperts are trademarks or registered trademarks of Devexperts LLC and/or its affiliates. All other product names, logos, and brands are property of their respective owners.

- Project source code license: The application code in this repository is provided under your organization’s chosen license or project-specific terms. If you intend to publish or redistribute this code, update this section with the appropriate open-source or proprietary license text and include the corresponding LICENSE file.
