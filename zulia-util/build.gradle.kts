plugins {
    `java-library`
}

description = "Zulia Util"

dependencies {
    api(libs.slf4j.api)
    api(libs.mongodb.driver.core)
    api(libs.guava)
    api(libs.bson)
    api(libs.gson)
}


