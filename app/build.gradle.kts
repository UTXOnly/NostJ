plugins {
    application
    java
    id("com.github.johnrengelman.shadow") version "8.1.1" // Shadow JAR plugin
}

group = "nostj"
version = "1.0.0"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

dependencies {
    implementation("jakarta.websocket:jakarta.websocket-api:2.1.0")

    // Netty for WebSockets
    implementation("io.netty:netty-all:4.1.100.Final")

    // Async Redis client (Lettuce)
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // Database (PostgreSQL)
    implementation("org.postgresql:postgresql:42.6.0")

    // Connection Pooling (HikariCP) - Fix missing class issue
    implementation("com.zaxxer:HikariCP:5.0.1")

    // JSON Parsing (Jackson)
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")

    // Logging (SLF4J + Logback)
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.8")

    // Unit Testing (JUnit 5)
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

application {
    mainClass.set("nostj.websockethandler.WebSocketApplication")
}

// ✅ Configure the Shadow JAR to include dependencies
tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    archiveClassifier.set("")  // Ensures output JAR is named without `-all`
    manifest {
        attributes["Main-Class"] = "nostj.websockethandler.WebSocketApplication"
    }
}

// ✅ Make the Shadow JAR the default JAR task
tasks.build {
    dependsOn(tasks.shadowJar)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
