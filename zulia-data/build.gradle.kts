plugins {
    `java-library`
}

description = "Zulia Data"


dependencies {
    api(libs.bundles.poi)
    api(libs.univocity.parsers)
    api(libs.jackson.databind)
    api(project(":zulia-util"))
}


