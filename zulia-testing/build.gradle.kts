plugins {
    `java-library`
}

description = "Zulia Testing"


dependencies {
    api(projects.zuliaUtil)
    api(projects.zuliaClient)
    implementation(libs.graalvm.js)
    implementation(libs.logback.classic)
    implementation(libs.jansi)
}

