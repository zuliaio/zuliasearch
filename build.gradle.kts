plugins {
    java
    idea
    signing
    `maven-publish`
}


allprojects {
    group = "io.zulia"
}




defaultTasks("build")
subprojects {

    plugins.apply("java")
    plugins.apply("idea")
    plugins.apply("signing")
    plugins.apply("maven-publish")
    plugins.apply("java-library")

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
        withSourcesJar()
        withJavadocJar()
    }

    publishing {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                artifact(tasks["sourcesJar"])
                artifact(tasks["javadocJar"])
                pom {
                    url.set("https://zulia.io")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("mdavis")
                            name.set("Matt Davis")
                            email.set("matt.davis@ascend-tech.us")
                            organization.set("Ascendant Software Technology, LLC")
                            organizationUrl.set("https://www.ascend-tech.us")
                        }
                        developer {
                            id.set("payam.meyer")
                            name.set("Payam Mayer")
                            email.set("payam.meyer@ascend-tech.us")
                            organization.set("Ascendant Software Technology, LLC")
                            organizationUrl.set("https://www.ascend-tech.us")
                        }
                    }
                    scm {
                        connection.set("git@github.com:zuliaio/zuliasearch.git")
                        developerConnection.set("git@github.com:zuliaio/zuliasearch.git")
                        url.set("https://github.com/zuliaio/zuliasearch")
                    }
                    name.set(project.name)
                    description.set(project.name)
                }
            }
        }
    }

    group = "io.zulia"

    dependencies {
        val catalogs = rootProject.extensions.getByType<VersionCatalogsExtension>()
        val libs = catalogs.named("libs")

        add("testImplementation", platform(libs.findLibrary("junit-bom").get()))
        add("testImplementation", libs.findLibrary("junit-jupiter").get())
        add("testRuntimeOnly", libs.findLibrary("junit-platform-launcher").get())

    }

    tasks.withType<Test> {
        useJUnitPlatform()

        /*
            By default, the unit tests will use an ephemeral, in-memory MongoDB instance for testing the logic.
            If an external mongoDB instance is required/needed, then uncomment the following line and define the connection
            URL for the target mongo instance
        */
        //systemProperty("mongoTestConnection", "mongodb://127.0.0.1:27017");

        workingDir = file("build/")
        jvmArgs = listOf("-Xmx1500m")

    }

}
