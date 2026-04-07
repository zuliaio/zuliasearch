description = "Zulia AI"

dependencies {
    api(projects.zuliaData)

    implementation(libs.sketches.java)
    implementation(libs.eclipse.collections)
    api(libs.djl.pytorch.engine)
    api(libs.djl.bom)
    api(libs.djl.huggingface.tokenizers)
    api(libs.djl.onnxruntime.engine)
    api(libs.djl.basicdataset)

    testRuntimeOnly(libs.logback.classic)
}



