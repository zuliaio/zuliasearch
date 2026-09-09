description = "Zulia Signals"

dependencies {
    api(projects.zuliaClient)

    testImplementation(testFixtures(projects.zuliaServer))
    testRuntimeClasspath(libs.logback.classic)
}

tasks.test {
    // the failure policy test opens a gRPC channel, and grpc-netty-shaded loads a native library
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    // SignalsNodeTest starts an in-process single node
    maxHeapSize = "3g"
}
