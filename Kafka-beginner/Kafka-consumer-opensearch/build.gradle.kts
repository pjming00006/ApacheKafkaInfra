plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.4.0")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.7")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.7")

    // https://central.sonatype.com/artifact/org.opensearch.client/opensearch-rest-high-level-client/2.7.0
    implementation("org.opensearch.client:opensearch-rest-high-level-client:1.2.4")

    // https://central.sonatype.com/artifact/com.google.code.gson/gson/2.10.1
    implementation("com.google.code.gson:gson:2.9.0")
}

tasks.test {
    useJUnitPlatform()
}