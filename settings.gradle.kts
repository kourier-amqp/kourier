rootProject.name = "kourier"
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}

include(":amqp-core")
include(":amqp-client")
include(":amqp-client-robust")
include(":amqp-client-opentelemetry")
