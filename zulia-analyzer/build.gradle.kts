plugins {
    `java-library`
}

description = "Zulia Analyzer"


val luceneVersion: String by project


defaultTasks("build", "installDist")

dependencies {
    api(project(":zulia-common"))

    api("org.apache.lucene:lucene-analysis-common:$luceneVersion")
    api("info.debatty:java-lsh:0.12")


}



