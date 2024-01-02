plugins {
    `java-library`
}

description = "Zulia Client"

val okHttpVersion: String by project
val gsonVersion: String by project

dependencies {
    api(project(":zulia-common"))
    implementation(libs.unirest.java.bom)
    implementation(libs.unirest.core)
    implementation(libs.unirest.gson)
}

