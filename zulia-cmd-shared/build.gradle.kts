description = "Zulia CMD Shared"

dependencies {
    api(projects.zuliaCommon)
    annotationProcessor(libs.picocli.codegen)
    implementation(libs.picocli.base)
}


