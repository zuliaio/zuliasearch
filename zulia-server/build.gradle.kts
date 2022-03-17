plugins {
    application
    `java-library`
}

description = "Zulia Server"

val luceneVersion: String by project
val mongoDriverVersion: String by project
val protobufVersion: String by project
val micronautVersion: String by project
val amazonVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(project(":zulia-query-parser"))
    api(project(":zulia-client")) //needed for admin tools

    api("org.apache.lucene:lucene-backward-codecs:$luceneVersion")
    api("org.apache.lucene:lucene-facet:$luceneVersion")
    api("org.apache.lucene:lucene-expressions:$luceneVersion")
    api("org.apache.lucene:lucene-highlighter:$luceneVersion")

    api("com.beust:jcommander:1.78")

    api("com.google.protobuf:protobuf-java-util:$protobufVersion")

    api("com.cedarsoftware:json-io:4.10.1")

    api("org.mongodb:mongodb-driver-sync:$mongoDriverVersion")

    api("org.apache.commons:commons-compress:1.20")
    implementation("org.xerial.snappy:snappy-java:1.1.8.4")
    implementation(platform("software.amazon.awssdk:bom:$amazonVersion"))
    implementation("software.amazon.awssdk:s3")

    annotationProcessor(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    annotationProcessor("io.micronaut:micronaut-inject-java")
    annotationProcessor("io.micronaut:micronaut-validation")
    implementation(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    implementation("io.micronaut:micronaut-inject")
    implementation("io.micronaut:micronaut-validation")
    implementation("io.micronaut.reactor:micronaut-reactor")
    implementation("io.micronaut:micronaut-runtime")
    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut:micronaut-http-client")
    runtimeOnly("ch.qos.logback:logback-classic")
    testAnnotationProcessor(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    testAnnotationProcessor("io.micronaut:micronaut-inject-java")
    testImplementation(platform("io.micronaut:micronaut-bom:$micronautVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("de.flapdoodle.embed:de.flapdoodle.embed.mongo:3.1.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    //implementation("org.graalvm.js:js:20.2.0")
}

val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts")
zuliaScriptTask.applicationName = "zulia"
zuliaScriptTask.mainClass.set("io.zulia.server.cmd.Zulia")

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


val zuliaFetchFileScriptTask = tasks.register<CreateStartScripts>("createZuliaFetchFileScript") {
    applicationName = "zuliafetchfile"
    mainClass.set("io.zulia.server.cmd.ZuliaFetchFile")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

val zuliaStoreFileScriptTask = tasks.register<CreateStartScripts>("createZuliaStoreFileScript") {
    applicationName = "zuliastorefile"
    mainClass.set("io.zulia.server.cmd.ZuliaStoreFile")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}

distributions {
    main {
        contents {
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
            from(zuliaFetchFileScriptTask) {
                into("bin")
            }
            from(zuliaStoreFileScriptTask) {
                into("bin")
            }
            fileMode = 777
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }
    }
}
