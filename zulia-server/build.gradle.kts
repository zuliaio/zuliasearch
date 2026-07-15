import io.zulia.task.PicoCLITask

import java.time.Duration

plugins {
    application
    alias(libs.plugins.micronaut.application)
}

description = "Zulia Server"

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

val copySwagger = tasks.register<Copy>("copySwagger") {
    description = "Copys Zulia Server REST Swagger to zulia-swagger server directory\""
    val compileJava = tasks.named<JavaCompile>(JavaPlugin.COMPILE_JAVA_TASK_NAME)
    val swaggerDir = compileJava.flatMap { it.destinationDirectory.dir("META-INF/swagger") }
    from(swaggerDir)
    into(rootProject.layout.projectDirectory.dir("zulia-swagger/server"))
}

tasks.named("build") {
    finalizedBy("copySwagger")
}

tasks.withType<Test> {
    maxParallelForks = 1
    maxHeapSize = "8g"
    this.testLogging {
        this.showStandardStreams = true
    }
}

tasks.test {
    useJUnitPlatform {
        excludeTags("soak")
    }
}

tasks.register<Test>("soakTest") {
    description = "Runs hour-scale soak tests tagged 'soak', which the regular test task excludes. Duration via -Dzulia.soak.minutes (default 60)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("soak")
    }
    systemProperty("zulia.soak.minutes", providers.systemProperty("zulia.soak.minutes").getOrElse("60"))
    timeout = Duration.ofHours(2)
    outputs.upToDateWhen { false }
}

dependencies {
    implementation(projects.zuliaCommon)
    implementation(projects.zuliaQueryParser)
    implementation(projects.zuliaCmdShared)
    annotationProcessor(libs.micronaut.http.validation)
    annotationProcessor(libs.micronaut.openapi)
    annotationProcessor(libs.micronaut.serde.processor)
    annotationProcessor(libs.micronaut.validation.processor)
    annotationProcessor(libs.micronaut.inject.java)
    annotationProcessor(libs.picocli.codegen)
    api(libs.lucene.backward.codecs)
    api(libs.lucene.expressions)
    api(libs.lucene.facet)
    api(libs.lucene.highlighter)
    api(libs.mongodb.driver.sync)
    implementation(libs.awssdk.s3)
    implementation(libs.caffeine)
    implementation(libs.commons.compress)
    implementation(libs.jakarta.validation)
    implementation(libs.jansi)
    implementation(libs.eclipse.collections)
    implementation(libs.logback.classic)
    implementation(libs.micronaut.http.base)
    implementation(libs.micronaut.http.server)
    implementation(libs.micronaut.management)
    implementation(libs.micronaut.openapi)
    implementation(libs.micronaut.reactor)
    implementation(libs.micronaut.serde.jackson)
    implementation(libs.picocli.base)
    implementation(libs.sketches.java)
    implementation(libs.snake.yaml)
    implementation(libs.snappy.java)
    implementation(libs.swagger.annotations)

    testImplementation(libs.flapdoodle.mongo)
    testImplementation(libs.junit.platform.launcher)
    testImplementation(libs.micronaut.http.client)
    testImplementation(libs.micronaut.test.junit5)
    testImplementation(projects.zuliaClient)
    testImplementation(projects.zuliaAi)
}
tasks.withType<JavaCompile> {
    options.isFork = true
    options.forkOptions.jvmArgs?.addAll(listOf("-Dmicronaut.openapi.views.spec=swagger-ui.enabled=true,swagger-ui.theme=flattop"))
}

application {
    applicationName = "zuliad"
    mainClass.set("io.zulia.server.cmd.ZuliaD")
    applicationDefaultJvmArgs = listOf( "--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
}

tasks.named<CreateStartScripts>("startScripts") {
    doLast {
        unixScript.writeText(
            unixScript.readText().replace("exec ", "export APP_HOME\nexec ")
        )
    }
}

val autocompleteTask = tasks.register<PicoCLITask>("autocomplete") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliad.sh")
    driverClass = "io.zulia.server.cmd.ZuliaD"
}

project.tasks.withType(PicoCLITask::class.java).configureEach {
    runtimeClasspath.from(project.configurations.runtimeClasspath, sourceSets.main.get().output)
}

distributions {
    main {
        distributionBaseName.set("zulia-server")
        contents {
            from(autocompleteTask.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }

            filePermissions {
                unix("777")
            }
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}

