plugins {
    java
}

// Emit Java 21 bytecode regardless of the local JDK (CI runs Temurin 21;
// local dev may run newer). Simpler than a toolchain + download-resolver.
tasks.withType<JavaCompile>().configureEach {
    options.release = 21
}

repositories {
    mavenCentral()
}

dependencies {
    // @Nullable / @NotNull for IDE analysis; compile-time only, no runtime footprint
    compileOnly("org.jetbrains:annotations:24.1.0")
    testCompileOnly("org.jetbrains:annotations:24.1.0")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("net.jqwik:jqwik:1.8.4")
}

// Default suite: deterministic, CI-green. The deliberately-failing KIP-101
// red variant and the anomaly-hunting fuzzer are excluded here and run via
// their own tasks below.
tasks.test {
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
        excludeTags("kip101-red", "fuzz-red")
    }
}

// The KIP-101 red test: committed telemetry vanishes under the pre-KIP-101
// truncate-to-HW rule. Expected to FAIL - that failure is the demo.
tasks.register<Test>("redTest") {
    group = "verification"
    description = "Runs the KIP-101 red variant (expected to fail while truncationRule=HIGH_WATERMARK)"
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
        includeTags("kip101-red")
    }
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
}

// Dev-only anomaly hunt: unpinned-seed fuzzing under the buggy rule.
tasks.register<Test>("fuzzRed") {
    group = "verification"
    description = "Biased schedule fuzzing under the buggy truncation rule (dev-only, not CI)"
    useJUnitPlatform {
        includeEngines("junit-jupiter", "jqwik")
        includeTags("fuzz-red")
    }
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
}
