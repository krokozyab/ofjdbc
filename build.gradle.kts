plugins {
    kotlin("jvm") version "2.0.21"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Use the Kotlin standard library from the plugin (version 2.0.21)
    implementation(kotlin("stdlib"))
    // Apache Commons Text for XML escaping/unescaping
    implementation("org.apache.commons:commons-text:1.10.0")
    // OkHttp for HTTP communication
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    //testImplementation(kotlin("test"))
    // SLF4J API for logging
    implementation("org.slf4j:slf4j-api:1.7.36")

    // A simple SLF4J binding (choose one that fits your needs; here we use slf4j-simple)
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")
}

tasks.test {
    useJUnitPlatform()
}

tasks {
    // Configure the ShadowJar (uberjar) task.
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        // Set no classifier so the resulting jar is named without "-all"
        archiveClassifier.set("")
        manifest {
            // Adjust the main class as necessary.
            attributes["Main-Class"] = "org.example.MainKt"
        }
        mergeServiceFiles {
            // Merge service files for auto‑discovery (for example, JDBC drivers)
            include("META-INF/services/java.sql.Driver")
        }
        // Exclude overlapping resources that cause warnings.
        exclude("META-INF/MANIFEST.MF")
        exclude("META-INF/LICENSE.txt")
        exclude("META-INF/NOTICE.txt")
        exclude("META-INF/okio.kotlin_module")
        exclude("META-INF/versions/9/module-info")
    }
}

kotlin {
    // Configure the JVM toolchain (adjust version as desired)
    jvmToolchain(21)
}
