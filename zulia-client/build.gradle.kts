plugins {
    `java-library`
}

description = "Zulia Client"

val micronautVersion: String by project

dependencies {
    api(project(":zulia-common"))
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("io.micronaut:micronaut-http-client:3.0.0") // forcing to working 3.0 until we switch to native java http client.
    implementation("io.micronaut.reactor:micronaut-reactor-http-client:2.0.0")
}

