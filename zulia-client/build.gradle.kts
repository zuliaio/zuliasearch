plugins {
    `java-library`
}

description = "Zulia Client"

val micronautVersion: String by project

dependencies {
    api(project(":zulia-common"))
    implementation("io.micronaut:micronaut-http-client:$micronautVersion")
}

