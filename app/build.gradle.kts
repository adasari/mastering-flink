/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.10.2/userguide/building_java_projects.html in the Gradle documentation.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("io.freefair.lombok") version "8.10.2"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven("https://repository.apache.org/content/repositories/snapshots/")
}

dependencies {
    implementation("org.apache.flink:flink-java:2.0-SNAPSHOT")  // Use the same version as your Flink Docker image
    implementation("org.apache.flink:flink-streaming-java:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-core:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-clients:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-connector-base:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-file-sink-common:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-connector-jdbc:3.2.0-1.19")
    implementation("org.apache.flink:flink-connector-datagen:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-avro:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-csv:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-connector-files:2.0-SNAPSHOT")

    implementation("org.apache.flink:flink-table-api-java:2.0-SNAPSHOT")
    implementation("org.apache.flink:flink-connector-postgres-cdc:3.2.0")



    implementation("org.postgresql:postgresql:42.7.4")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
    implementation("org.apache.logging.log4j:log4j-api:2.17.1")
    implementation("org.apache.logging.log4j:log4j-core:2.17.1")
    implementation("org.apache.logging.log4j:log4j-1.2-api:2.17.1")
    implementation("org.slf4j:slf4j-api:1.7.36")

//    implementation("org.apache.flink:flink-scala_2.12:1.20.0")  // Use the same version as your Flink Docker image
//    implementation("org.apache.flink:flink-streaming-scala_2.12:1.20.0")
//    implementation("org.scala-lang:scala-library:2.12.8")       // Scala version compatible with Flink
}


tasks.withType<JavaCompile> {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

application {
    // Define the main class for the application.
    mainClass = "org.example.paralleljdbc.JdbcSourceSplitExample"
}

tasks.withType<ShadowJar> {
    archiveBaseName.set("app-uber") // Set the base name for your jar
    archiveClassifier.set("")         // No classifier for the main jar
    archiveVersion.set("")            // If you want to omit the version number in the jar name
    mergeServiceFiles()               // Merge service files (e.g., for META-INF/services)
    manifest {
        attributes["Main-Class"] = "org.example.paralleljdbc.JdbcSourceSplitExample" // Replace with your main class
    }
}