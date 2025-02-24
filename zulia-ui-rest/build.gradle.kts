plugins {
    application
    `java-library`
    alias(libs.plugins.micronaut.application)
}

description = "Zulia UI REST"

defaultTasks("build", "installDist")

micronaut {
    version(libs.versions.micronaut.get())
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("io.zulia.*")
    }
}

tasks.register<Copy>("copySwagger") {
    from(layout.buildDirectory.dir("classes/java/main/META-INF/swagger/"))
    into(project.rootProject.file("zulia-swagger/"))
    dependsOn("buildLayers")
}

tasks.withType<Test> {
    maxParallelForks = 1
    maxHeapSize = "8g"
    this.testLogging {
        this.showStandardStreams = true
    }
}

dependencies {
    implementation(projects.zuliaQueryParser)
    implementation(projects.zuliaCmdShared)
    annotationProcessor(libs.micronaut.http.validation)
    annotationProcessor(libs.micronaut.openapi)
    annotationProcessor(libs.micronaut.serde.processor)
    annotationProcessor(libs.micronaut.validation.processor)
    annotationProcessor(libs.micronaut.data.processor)
    annotationProcessor(libs.micronaut.data.document.processor)
    annotationProcessor(libs.picocli.codegen)
    annotationProcessor(libs.micronaut.openapi)
    api(libs.mongodb.driver.sync)
    implementation(libs.awssdk.s3)
    implementation(libs.caffeine)
    implementation(libs.commons.compress)
    implementation(libs.jakarta.validation)
    implementation(libs.jansi)
    implementation(libs.json.io)
    implementation(libs.koloboke.api)
    implementation(libs.koloboke.impl)
    implementation(libs.logback.classic)
    implementation(libs.micronaut.http.base)
    implementation(libs.micronaut.http.server)
    implementation(libs.micronaut.inject.java)
    implementation(libs.micronaut.management)
    implementation(libs.micronaut.reactor)
    implementation(libs.micronaut.serde.jackson)
    implementation(libs.micronaut.security.jwt)
    implementation(libs.micronaut.openapi)
    implementation(libs.micronaut.data.mongodb)
    implementation(libs.sketches.java)
    implementation(libs.snake.yaml)
    implementation(libs.snappy.java)
    implementation(libs.swagger.annotations)

    testImplementation(libs.flapdoodle.mongo)
    testImplementation(libs.micronaut.http.client)
    testImplementation(libs.micronaut.test.junit5)
    testImplementation(project(":zulia-client"))
    testImplementation(libs.micronaut.test.security)
    testImplementation(libs.jakarta.inject)
}

tasks.withType<JavaCompile> {
    options.isFork = true
    options.forkOptions.jvmArgs?.addAll(listOf("-Dmicronaut.openapi.views.spec=swagger-ui.enabled=true,swagger-ui.theme=flattop"))
}
