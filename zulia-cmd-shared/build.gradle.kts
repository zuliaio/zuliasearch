plugins {
    `java-library`
}

description = "Zulia CMD Shared"

dependencies {
    annotationProcessor(libs.picocli.codegen)
    api(projects.zuliaCommon)
    implementation(libs.picocli.base)
}


