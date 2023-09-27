plugins {
    `java-library`
}

description = "Zulia Query Parser"

val luceneVersion: String by project

defaultTasks("build", "installDist")

dependencies {
    api(project(":zulia-analyzer"))
    api("org.apache.lucene:lucene-queryparser:$luceneVersion")
}



