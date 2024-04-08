plugins {
    `java-library`
}

description = "Zulia Query Parser"

val luceneVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(projects.zuliaAnalyzer)
    api(libs.lucene.queryparser)
}



