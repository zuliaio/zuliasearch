plugins {
    application
    `java-library`
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
    implementation(libs.picocli.base)
    implementation(libs.sketches.java)
    implementation(libs.snake.yaml)
    implementation(libs.snappy.java)
    implementation(libs.swagger.annotations)

    testImplementation(libs.flapdoodle.mongo)
    testImplementation(libs.micronaut.http.client)
    testImplementation(libs.micronaut.test.junit5)
    testImplementation(project(":zulia-client"))
}
tasks.withType<JavaCompile> {
    options.isFork = true
    options.forkOptions.jvmArgs?.addAll(listOf("-Dmicronaut.openapi.views.spec=swagger-ui.enabled=true,swagger-ui.theme=flattop"))
}


val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts") {
    applicationName = "zuliad"
    mainClass.set("io.zulia.server.cmd.ZuliaD")
    defaultJvmOpts = listOf("--add-modules","jdk.incubator.vector","--enable-native-access=ALL-UNNAMED")
    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("exec ", "export APP_HOME\nexec ")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}



tasks.register("autocompleteDir") {
    dependsOn(":zulia-common:version")
    doLast {
        mkdir("${layout.buildDirectory.get()}/autocomplete")
    }
}

task("picoCliZuliaDAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliad.sh",
        "io.zulia.server.cmd.ZuliaD"
    )
}


tasks.withType<AbstractArchiveTask> {
    dependsOn(
        "picoCliZuliaDAutoComplete",
    )
}


distributions {
    main {
        contents {
            from("${layout.buildDirectory.get()}/autocomplete/") {
                into("bin/autocomplete")
            }

            filePermissions {
                unix("777")
            }
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}

