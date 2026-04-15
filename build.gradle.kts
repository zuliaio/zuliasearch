plugins {
    java
    idea
    alias(libs.plugins.maven.publish) apply false
    id("com.github.ben-manes.versions") version "0.53.0"
    id("org.owasp.dependencycheck") version "12.2.0"
}

dependencyCheck {
    failBuildOnCVSS = 7.0f
    formats = listOf("HTML", "JSON")
}


allprojects {
    group = "io.zulia"
}

fun isNonStable(version: String): Boolean {
    val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.uppercase().contains(it) }
    val regex = "^[0-9,.v-]+(-r)?$".toRegex()
    val isStable = stableKeyword || regex.matches(version)
    return isStable.not()
}

tasks.withType<com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask> {
    rejectVersionIf {
        isNonStable(candidate.version) && !isNonStable(currentVersion)
    }
}




defaultTasks("build")
subprojects {

    plugins.apply("java")
    plugins.apply("idea")
    plugins.apply("java-library")
    plugins.apply("com.vanniktech.maven.publish")

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = true
    }

    configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral(automaticRelease = true)
        signAllPublications()

        pom {
            name.set(project.name)
            description.set(project.name)
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
                connection.set("scm:git:https://github.com/zuliaio/zuliasearch.git")
                developerConnection.set("scm:git:ssh://git@github.com/zuliaio/zuliasearch.git")
                url.set("https://github.com/zuliaio/zuliasearch")
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
