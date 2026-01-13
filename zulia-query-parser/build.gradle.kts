description = "Zulia Query Parser"

dependencies {
    api(projects.zuliaAnalyzer)
    api(libs.lucene.queryparser)

    testRuntimeClasspath(libs.logback.classic)
    testRuntimeClasspath(libs.jansi)
}



