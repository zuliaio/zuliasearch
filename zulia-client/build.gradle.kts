description = "Zulia Client"

dependencies {
    api(projects.zuliaCommon)
    api(projects.zuliaUtil)
    implementation(libs.unirest.java.bom)
    implementation(libs.unirest.core)
    implementation(libs.unirest.gson)
}

