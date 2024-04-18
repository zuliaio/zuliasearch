plugins {
    `java-library`
}

description = "Zulia Analyzer"

val luceneVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(projects.zuliaCommon)
    api(libs.lucene.analysis.common)
}



