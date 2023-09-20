plugins {
    application
    `java-library`
    id("io.micronaut.application") version ("4.0.2")
}

description = "Zulia Server"

val luceneVersion: String by project
val mongoDriverVersion: String by project
val micronautVersion: String by project
val amazonVersion: String by project
val snakeYamlVersion: String by project
val kolobokeVersion: String by project

defaultTasks("build", "installDist")


micronaut {
    version(micronautVersion)
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("io.zulia.*")
    }
}


tasks.withType<Test> {
    maxParallelForks = 1
    maxHeapSize = "8g"
    this.testLogging {
        this.showStandardStreams = true
    }
}

dependencies {
    implementation(project(":zulia-query-parser"))
    implementation(project(":zulia-client")) //needed for admin tools

    implementation("org.apache.lucene:lucene-backward-codecs:$luceneVersion")
    implementation("org.apache.lucene:lucene-facet:$luceneVersion")
    implementation("org.apache.lucene:lucene-expressions:$luceneVersion")
    implementation("org.apache.lucene:lucene-highlighter:$luceneVersion")

    implementation("com.github.ben-manes.caffeine:caffeine")

    implementation("com.koloboke:koloboke-api-jdk8:$kolobokeVersion")
    implementation("com.koloboke:koloboke-impl-jdk8:$kolobokeVersion")

    implementation("com.datadoghq:sketches-java:0.8.2")
    implementation("com.cedarsoftware:json-io:4.14.0")
    implementation("org.mongodb:mongodb-driver-sync:$mongoDriverVersion")
    implementation("org.apache.commons:commons-compress:1.24.0")
    implementation("org.xerial.snappy:snappy-java:1.1.10.3")
    implementation("software.amazon.awssdk:s3")

    implementation("org.yaml:snakeyaml:$snakeYamlVersion")

    runtimeOnly("ch.qos.logback:logback-classic")
    implementation("org.fusesource.jansi:jansi:2.4.0")


    implementation("info.picocli:picocli:4.7.5")
    annotationProcessor("info.picocli:picocli-codegen:4.7.5")

    annotationProcessor("io.micronaut:micronaut-http-validation")
    annotationProcessor("io.micronaut.serde:micronaut-serde-processor")
    annotationProcessor("io.micronaut.openapi:micronaut-openapi")
    annotationProcessor("io.micronaut.validation:micronaut-validation-processor")
    implementation("io.micronaut:micronaut-http-server")
    implementation("io.micronaut:micronaut-http")
    implementation("io.micronaut:micronaut-inject-java")
    implementation("io.micronaut:micronaut-management")
    implementation("io.micronaut.openapi:micronaut-openapi")
    implementation("io.micronaut.reactor:micronaut-reactor")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    implementation("io.micronaut.validation:micronaut-validation")
    implementation("io.swagger.core.v3:swagger-annotations")
    implementation("jakarta.validation:jakarta.validation-api")

    testAnnotationProcessor("io.micronaut:micronaut-inject-java")
    testImplementation("io.micronaut:micronaut-http-client")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("de.flapdoodle.embed:de.flapdoodle.embed.mongo:4.9.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")


    //implementation("org.graalvm.js:js:21.3.1")
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
        mkdir("$buildDir/autocomplete")
    }
}

task("picoCliZuliaAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf("--force", "--completionScript", "$buildDir/autocomplete/zulia.sh", "io.zulia.server.cmd.Zulia")
}

task("picoCliZuliaDAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf("--force", "--completionScript", "$buildDir/autocomplete/zuliad.sh", "io.zulia.server.cmd.ZuliaD")
}

task("picoCliZuliaAdminAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "$buildDir/autocomplete/zuliaadmin.sh",
        "io.zulia.server.cmd.ZuliaAdmin"
    )
}

task("picoCliZuliaDumpAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args =
        listOf("--force", "--completionScript", "$buildDir/autocomplete/zuliadump.sh", "io.zulia.server.cmd.ZuliaDump")
}

task("picoCliZuliaRestoreAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "$buildDir/autocomplete/zuliarestore.sh",
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
        "$buildDir/autocomplete/zuliaimport.sh",
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
        "$buildDir/autocomplete/zuliaexport.sh",
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
            from("$buildDir/autocomplete/") {
                into("bin/autocomplete")
            }

            fileMode = 777
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}

