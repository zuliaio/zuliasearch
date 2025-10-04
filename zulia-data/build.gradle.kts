description = "Zulia Data"

dependencies {
    api(projects.zuliaUtil)
    api(libs.bundles.poi)
    api(libs.commons.compress)
    api(libs.univocity.parsers)
    api(libs.jackson.databind)

    testRuntimeClasspath(libs.logback.classic)
    testRuntimeClasspath(libs.jansi)
}


