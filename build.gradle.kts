// Gradle build file with plugins and dependencies pinned to explicit versions

plugins {
    kotlin("jvm") version "2.2.20-Beta2"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("com.github.ben-manes.versions") version "0.52.0"
}

group = "my.jdbc"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib", "2.2.20-Beta2"))
    implementation("org.apache.commons:commons-text:1.14.0")
    implementation("org.slf4j:slf4j-api:2.1.0-alpha1")
    implementation("org.duckdb:duckdb_jdbc:1.3.2.0")
    implementation("com.fasterxml.woodstox:woodstox-core:6.5.1")
    implementation("org.ccil.cowan.tagsoup:tagsoup:1.2.1")
    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.wrapper {
    gradleVersion = "8.8"
}


tasks {
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        archiveClassifier.set("")
        mergeServiceFiles {
            include("META-INF/services/java.sql.Driver")
        }
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
