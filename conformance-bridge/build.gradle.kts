plugins {
    kotlin("jvm")
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val coroutinesVersion: String by project

dependencies {
    implementation(project(":lxmf-core"))
    // Must match the rns-core/rns-interfaces version :lxmf-core depends on. A mismatch here
    // ships the bridge an older Resource implementation than the tests running against it
    // exercise, which has bitten this module before — so these move with lxmf-core, always.
    implementation("com.github.kishontivf.reticulum-kt:rns-core:0.1.0")
    implementation("com.github.kishontivf.reticulum-kt:rns-interfaces:0.1.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.json:json:20231013")

    // msgpack-core for the byte-level lxmf_decode_bytes command, which
    // operates directly on the wire format rather than going through
    // the LXMessage class. Same version as :lxmf-core uses internally.
    implementation("org.msgpack:msgpack-core:0.9.8")

    // Logging — use slf4j-simple but at WARN by default so RNS chatter
    // doesn't interleave with the JSON-RPC stdout protocol.
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("org.slf4j:slf4j-simple:2.0.9")

    testImplementation(kotlin("test"))
}

application {
    mainClass.set("network.reticulum.lxmf.conformance.MainKt")
}

tasks {
    shadowJar {
        // Produce build/libs/LXMFConformanceBridge.jar (no version suffix).
        archiveBaseName.set("LXMFConformanceBridge")
        archiveClassifier.set("")
        archiveVersion.set("")
        manifest {
            attributes["Main-Class"] = "network.reticulum.lxmf.conformance.MainKt"
        }
        // SLF4J simple at WARN to keep stdout clean for JSON-RPC.
        // The bridge sets these at runtime too, but pinning here is belt-and-suspenders.
        mergeServiceFiles()
    }

    // `gradle build` in this subproject should produce the runnable shadow jar.
    build {
        dependsOn(shadowJar)
    }

    // Disable the plain jar to avoid confusing developers about which artifact runs.
    named<Jar>("jar") {
        enabled = false
    }
}
