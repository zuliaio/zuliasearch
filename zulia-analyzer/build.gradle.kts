plugins {
    `java-library`
}

description = "Zulia Analyzer"

val luceneVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(project(":zulia-common"))
    api(libs.lucene.analysis.common)
}



