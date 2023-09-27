plugins {
    `java-library`
}

description = "Zulia Client"

val okHttpVersion: String by project
val gsonVersion: String by project

dependencies {
    api(project(":zulia-common"))
    implementation("com.squareup.okhttp3:okhttp:$okHttpVersion")
}

