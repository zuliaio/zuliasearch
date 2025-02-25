plugins {
    `java-library`
}

description = "Zulia AI"


defaultTasks("build", "installDist")

dependencies {
    api(projects.zuliaData)
    implementation(libs.logback.classic)
    implementation(libs.sketches.java)
    implementation(libs.koloboke.api)
    implementation(libs.koloboke.impl)
    api(libs.djl.pytorch.engine)
    api(libs.djl.bom)
    //api(libs.djl.huggingface.tokenizers)
    api(libs.djl.basicdataset)
}



