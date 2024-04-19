plugins {
    `java-library`
}

description = "Zulia Data"


dependencies {
    api(libs.bundles.poi)
    api(libs.commons.compress)
    api(libs.univocity.parsers)
    api(libs.jackson.databind)
    api(projects.zuliaUtil)
}


