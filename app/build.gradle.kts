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
    // ✅ OpenTelemetry Core API
    implementation("io.opentelemetry:opentelemetry-api:1.37.0")

    // ✅ OpenTelemetry SDK for custom manual tracing
    implementation("io.opentelemetry:opentelemetry-sdk:1.37.0")
    implementation("io.opentelemetry:opentelemetry-sdk-trace:1.37.0")

    // ✅ OpenTelemetry OTLP Exporter (optional, remove if not needed)
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.37.0")
    implementation("io.opentelemetry.semconv:opentelemetry-semconv:1.30.0")



    // ✅ Database & Redis
    implementation("io.vertx:vertx-pg-client:4.5.1")
    implementation ("com.ongres.scram:client:2.1")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // ✅ JSON Processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")

    // ✅ Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // ✅ Unit Testing (JUnit 5)
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
