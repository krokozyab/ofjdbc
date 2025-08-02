// All dependencies configured to use latest versions with dynamic versioning

plugins {
    kotlin("jvm") version "+"
    id("com.github.johnrengelman.shadow") version "+"
    id("com.github.ben-manes.versions") version "+"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Use the Kotlin standard library from the plugin
    implementation(kotlin("stdlib"))
    // Apache Commons Text for XML escaping/unescaping
    implementation("org.apache.commons:commons-text:+")
    implementation("org.slf4j:slf4j-api:+")
    implementation("org.duckdb:duckdb_jdbc:+")
    // A simple SLF4J binding (choose one that fits your needs; here we use slf4j-simple)
    runtimeOnly("org.slf4j:slf4j-simple:+")

    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:+")
}

tasks.test {
    useJUnitPlatform()
}

tasks.wrapper {
    gradleVersion = "+"
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
    jvmToolchain(11)
}
