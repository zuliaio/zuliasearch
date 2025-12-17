description = "Zulia Data"

dependencies {
    api(projects.zuliaUtil)
    api(libs.bundles.poi)
    api(libs.commons.compress)
    api(libs.fast.csv)
    api(libs.jackson.databind)

    testRuntimeClasspath(libs.logback.classic)
    testRuntimeClasspath(libs.jansi)
}


