plugins {
    `java-library`
}

description = "Zulia Client"

dependencies {
    api(project(":zulia-common"))
    implementation("io.micronaut:micronaut-http-client:1.2.10")
}

