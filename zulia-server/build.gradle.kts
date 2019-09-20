plugins {
    application
}

description = "Zulia Server"

val luceneVersion: String by project
val mongoDriverVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    compile(project(":zulia-query-parser"))
    compile(project(":zulia-client")) //needed for admin tools

    compile("org.apache.lucene:lucene-backward-codecs:$luceneVersion")
    compile("org.apache.lucene:lucene-facet:$luceneVersion")

    compile("org.apache.lucene:lucene-highlighter:$luceneVersion")

    compile("com.beust:jcommander:1.78")

    compile("org.glassfish.jersey.containers:jersey-container-grizzly2-http:2.27")
    compile("org.glassfish.jersey.inject:jersey-hk2:2.27")

    compile("javax.activation:activation:1.1.1")
    compile("javax.xml.bind:jaxb-api:2.3.0")
    compile("javax.annotation:javax.annotation-api:1.3.2")

    compile("com.google.protobuf:protobuf-java-util:3.7.1")

    compile("com.cedarsoftware:json-io:4.10.1")

    compile("org.mongodb:mongodb-driver-sync:$mongoDriverVersion")
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