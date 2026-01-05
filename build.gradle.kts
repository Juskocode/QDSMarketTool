plugins {
    id("java")
    // Provide `run` task to execute the tool from Gradle
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    // Pick up ALL local jars from libs/ (dxfeed-api, qds, and any additional Devexperts IO jars)
    // This ensures ./gradlew run and the shadow fat jar have every required class on the classpath.
    implementation(fileTree(mapOf("dir" to "libs", "include" to listOf("*.jar"))))

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-inline:5.2.0")
    // jqwik for property-based testing on JUnit Platform
    testImplementation("net.jqwik:jqwik:1.8.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}


tasks.test {
    useJUnitPlatform()
}

// Note: We intentionally do not apply the 'application' plugin by default to keep :test clean from
// deprecation warnings coming from distribution/start scripts configuration.
// If you need 'run' or distributions, you can re-enable the plugin or use the shadow jar.

// Configure jar manifest for the standard jar task
tasks.named<Jar>("jar") {
    manifest {
        attributes["Main-Class"] = "org.example.MarketScheduleUpdater"
    }
}

// If the build is invoked with any shadow task, apply the plugin then (keeps :test clean)
if (gradle.startParameter.taskNames.any { it.contains("shadow", ignoreCase = true) }) {
    apply(plugin = "com.github.johnrengelman.shadow")
    // Optional: you can configure shadowJar here if needed
}

application {
    mainClass.set("org.example.MarketScheduleUpdater")
}

// Convenience: when no args are provided to `run`, default to the repo-local conf and out paths under resources
tasks.named<JavaExec>("run") {
    // Only set default args if not provided via --args on the command line
    if (!project.hasProperty("args")) {
        args = listOf("--markets", "conf/markets.list")
    }
}

// Dedicated runner for GoldenVectorGenerator
tasks.register<JavaExec>("runGolden") {
    group = "application"
    description = "Run the GoldenVectorGenerator to produce per-minute golden vectors for tv.*"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.example.GoldenVectorGenerator")
    // No default args; pass via --args="..." on the command line
}