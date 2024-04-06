plugins {
    `java-library`
}

description = "Zulia CMD Shared"

dependencies {
    annotationProcessor(libs.picocli.codegen)
    api(project(":zulia-common"))
    implementation(libs.picocli.base)
}


