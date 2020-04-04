plugins {
    application
    `java-library`
}

description = "Zulia Server"

val luceneVersion: String by project
val mongoDriverVersion: String by project
val protobufVersion: String by project
val micronautVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(project(":zulia-query-parser"))
    api(project(":zulia-client")) //needed for admin tools

    api("org.apache.lucene:lucene-backward-codecs:$luceneVersion")
    api("org.apache.lucene:lucene-facet:$luceneVersion")

    api("org.apache.lucene:lucene-highlighter:$luceneVersion")

    api("com.beust:jcommander:1.78")

    api("org.glassfish.jersey.containers:jersey-container-grizzly2-http:2.27")
    api("org.glassfish.jersey.inject:jersey-hk2:2.27")

    api("javax.activation:activation:1.1.1")
    api("javax.xml.bind:jaxb-api:2.3.0")
    api("javax.annotation:javax.annotation-api:1.3.2")

    api("com.google.protobuf:protobuf-java-util:$protobufVersion")

    api("com.cedarsoftware:json-io:4.10.1")

    api("org.mongodb:mongodb-driver-sync:$mongoDriverVersion")

    api("org.apache.commons:commons-compress:1.20")

    implementation("io.reactivex.rxjava2:rxjava:2.2.0")

    annotationProcessor("io.micronaut:micronaut-inject-java:$micronautVersion")
    implementation("io.micronaut:micronaut-http-client:$micronautVersion")
    implementation("io.micronaut:micronaut-http-server-netty:$micronautVersion")
    implementation("io.micronaut:micronaut-inject:$micronautVersion")
    implementation("io.micronaut:micronaut-runtime:$micronautVersion")

    compileOnly("io.micronaut:micronaut-inject-java:$micronautVersion")
    testCompile("junit:junit:4.12")

    implementation("javax.annotation:javax.annotation-api:1.3.2")
    annotationProcessor("javax.annotation:javax.annotation-api:1.3.2")
}

val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts")
zuliaScriptTask.applicationName = "zulia"
zuliaScriptTask.mainClassName = "io.zulia.server.cmd.Zulia"

val zuliaDScriptTask = tasks.register<CreateStartScripts>("createZuliaDScript") {
    applicationName = "zuliad"
    mainClassName = "io.zulia.server.cmd.ZuliaD"
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
    mainClassName = "io.zulia.server.cmd.ZuliaDump"
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
    mainClassName = "io.zulia.server.cmd.ZuliaRestore"
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
    mainClassName = "io.zulia.server.cmd.ZuliaExport"
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
    mainClassName = "io.zulia.server.cmd.ZuliaImport"
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
            fileMode = 777
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }
    }
}