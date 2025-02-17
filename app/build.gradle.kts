plugins {
    id("org.springframework.boot") version "3.2.0"
    id("io.spring.dependency-management") version "1.1.4"
    application
    java
}

group = "nostj"
version = "1.0.0"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot Dependencies
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")

    // WebSockets API
    implementation("jakarta.websocket:jakarta.websocket-api:2.1.0")

    // Database Driver
    implementation("org.postgresql:postgresql:42.6.0")

    // Async Redis client
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
    
    // Redis (Jedis) for Pub/Sub
    implementation("redis.clients:jedis:5.1.0")

    // JSON Parsing (Jackson)
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")

    // HTTP Client (for calling Event Handler)
    implementation("org.apache.httpcomponents.client5:httpclient5:5.3")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.8")

    // Spring Boot DevTools (Optional for development)
    developmentOnly("org.springframework.boot:spring-boot-devtools")

    // Unit Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

application {
    mainClass.set("nostj.websockethandler.WebSocketApplication")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
