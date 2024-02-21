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
    from(layout.buildDirectory.dir("classes/java/main/META-INF/swagger/swagger.yml"))
    into(project.rootProject.file("api/"))
    rename("swagger.yml", project.name + "_swagger.yml")
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
    implementation(project(":zulia-client")) //needed for admin tools
    implementation(project(":zulia-query-parser"))
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
}
tasks.withType<JavaCompile> {
    options.isFork = true
    options.forkOptions.jvmArgs?.addAll(listOf("-Dmicronaut.openapi.views.spec=swagger-ui.enabled=true,swagger-ui.theme=flattop"))
}

val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts")


zuliaScriptTask.applicationName = "zulia"
zuliaScriptTask.mainClass.set("io.zulia.server.cmd.Zulia")
val zuliaAdminScriptTask = tasks.register<CreateStartScripts>("createZuliaAdminScript") {
    applicationName = "zuliaadmin"
    mainClass.set("io.zulia.server.cmd.ZuliaAdmin")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaDScriptTask = tasks.register<CreateStartScripts>("createZuliaDScript") {
    applicationName = "zuliad"
    mainClass.set("io.zulia.server.cmd.ZuliaD")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaDumpScriptTask = tasks.register<CreateStartScripts>("createZuliaDumpScript") {
    applicationName = "zuliadump"
    mainClass.set("io.zulia.server.cmd.ZuliaDump")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaRestoreScriptTask = tasks.register<CreateStartScripts>("createZuliaRestoreScript") {
    applicationName = "zuliarestore"
    mainClass.set("io.zulia.server.cmd.ZuliaRestore")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaExportScriptTask = tasks.register<CreateStartScripts>("createZuliaExportScript") {
    applicationName = "zuliaexport"
    mainClass.set("io.zulia.server.cmd.ZuliaExport")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaImportScriptTask = tasks.register<CreateStartScripts>("createZuliaImportScript") {
    applicationName = "zuliaimport"
    mainClass.set("io.zulia.server.cmd.ZuliaImport")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

tasks.register("autocompleteDir") {
    doLast {
        mkdir("${layout.buildDirectory.get()}/autocomplete")
    }
}

task("picoCliZuliaAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf("--force", "--completionScript", "${layout.buildDirectory.get()}/autocomplete/zulia.sh", "io.zulia.server.cmd.Zulia")
}

task("picoCliZuliaDAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf("--force", "--completionScript", "${layout.buildDirectory.get()}/autocomplete/zuliad.sh", "io.zulia.server.cmd.ZuliaD")
}

task("picoCliZuliaAdminAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaadmin.sh",
        "io.zulia.server.cmd.ZuliaAdmin"
    )
}

task("picoCliZuliaDumpAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args =
        listOf("--force", "--completionScript", "${layout.buildDirectory.get()}/autocomplete/zuliadump.sh", "io.zulia.server.cmd.ZuliaDump")
}



task("picoCliZuliaRestoreAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliarestore.sh",
        "io.zulia.server.cmd.ZuliaRestore"
    )
}


task("picoCliZuliaImportAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaimport.sh",
        "io.zulia.server.cmd.ZuliaImport"
    )
}

task("picoCliZuliaExportAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaexport.sh",
        "io.zulia.server.cmd.ZuliaExport"
    )
}

tasks.withType<AbstractArchiveTask> {
    dependsOn(
        "picoCliZuliaAutoComplete",
        "picoCliZuliaDAutoComplete",
        "picoCliZuliaAdminAutoComplete",
        "picoCliZuliaDumpAutoComplete",
        "picoCliZuliaRestoreAutoComplete",
        "picoCliZuliaImportAutoComplete",
        "picoCliZuliaExportAutoComplete"
    )
}


distributions {
    main {
        contents {
            from(zuliaAdminScriptTask) {
                into("bin")
            }
            from(zuliaDScriptTask) {
                into("bin")
            }
            from(zuliaDumpScriptTask) {
                into("bin")
            }
            from(zuliaRestoreScriptTask) {
                into("bin")
            }
            from(zuliaExportScriptTask) {
                into("bin")
            }
            from(zuliaImportScriptTask) {
                into("bin")
            }
            from("${layout.buildDirectory.get()}/autocomplete/") {
                into("bin/autocomplete")
            }

            fileMode = 777
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}

