description = "Zulia Query Parser"


val luceneVersion: String by project


defaultTasks("build", "installDist")

dependencies {
    compile(project(":zulia-analyzer"))
    compile("org.apache.lucene:lucene-queryparser:$luceneVersion")

}



