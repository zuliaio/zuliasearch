description = "Zulia Data"

dependencies {
    api(projects.zuliaUtil)
    api(libs.poi)
    api(libs.poi.ooxml)
    api(libs.commons.compress)
    api(libs.fast.csv)
    api(libs.jackson.databind)

    testRuntimeClasspath(libs.logback.classic)
}


