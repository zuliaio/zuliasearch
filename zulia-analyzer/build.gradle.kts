description = "Zulia Analyzer"


val luceneVersion: String by project


defaultTasks("build", "installDist")

dependencies {
    compile(project(":zulia-common"))


    compile("org.apache.lucene:lucene-analyzers-common:$luceneVersion")
    compile("info.debatty:java-lsh:0.12")


}



