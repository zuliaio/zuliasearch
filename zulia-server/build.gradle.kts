plugins {
    application
}

description = "Zulia Server"


val luceneVersion = "7.5.0"


defaultTasks("build", "installDist")

dependencies {
    compile(project(":zulia-common"))
    compile(project(":zulia-client")) //needed for admin tools

    compile("org.apache.lucene:lucene-facet:$luceneVersion")
    compile("org.apache.lucene:lucene-queryparser:$luceneVersion")
    compile("org.apache.lucene:lucene-analyzers-common:$luceneVersion")
    compile("org.apache.lucene:lucene-highlighter:$luceneVersion")

    compile("com.beust:jcommander:1.72")

    compile("org.glassfish.jersey.containers:jersey-container-grizzly2-http:2.27")
    compile("org.glassfish.jersey.inject:jersey-hk2:2.27")

    compile("javax.activation:activation:1.1.1")
    compile("javax.xml.bind:jaxb-api:2.3.0")
    compile("javax.annotation:javax.annotation-api:1.3.2")

    compile("com.google.protobuf:protobuf-java-util:3.6.1")

    compile("com.cedarsoftware:json-io:4.10.0")

    compile("info.debatty:java-lsh:0.11")

    compile("org.mongodb:mongodb-driver-sync:3.9.0")

}


val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts")
zuliaScriptTask.applicationName = "zulia"
zuliaScriptTask.mainClassName = "io.zulia.server.cmd.Zulia"


val zuliaDScriptTask = tasks.register<CreateStartScripts>("createZuliaDScript") {
    applicationName = "zuliad"
    mainClassName = "io.zulia.server.cmd.ZuliaD"
    //outputDir = zuliaScriptTask.outputDir
    //classpath = zuliaScriptTask.classpath

    doLast {
        val unixScriptFile = file(unixScript)
        val text = unixScriptFile.readText(Charsets.UTF_8)
        val newText = text.replace("APP_HOME=\"`pwd -P`\"'", "export APP_HOME=\"`pwd -P`\"")
        unixScriptFile.writeText(newText, Charsets.UTF_8)
    }
}


//applicationDistribution.into("bin") {
//    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
//    from(createZuliaDScript)
//    fileMode = 0755
//}

