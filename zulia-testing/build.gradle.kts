plugins {
    `java-library`
}

description = "Zulia Testing"


dependencies {
    api(project(":zulia-util"))
    api(project(":zulia-client"))
    implementation(libs.snake.yaml)
    implementation(libs.graalvm.js)
    implementation(libs.logback.classic)
    implementation(libs.jansi)
}

