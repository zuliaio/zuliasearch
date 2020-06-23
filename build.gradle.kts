import org.ajoberstar.reckon.gradle.ReckonExtension

plugins {
    java
    maven
    idea
    signing
    `maven-publish`
    id("org.ajoberstar.reckon") version "0.12.0"
    id("com.google.protobuf") version "0.8.12" apply false
}

configure<ReckonExtension> {
    scopeFromProp()
    stageFromProp("rc", "final")
}

allprojects {
    group = "io.zulia"
}

defaultTasks("build")
subprojects {

    apply(plugin = "java")
    apply(plugin = "maven")
    apply(plugin = "idea")
    apply(plugin = "signing")
    apply(plugin = "maven-publish")

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    val sourcesJar = tasks.register<Jar>("sourcesJar") {
        dependsOn(JavaPlugin.CLASSES_TASK_NAME)
        archiveClassifier.set("sources")
        from(sourceSets.main.get().allJava)
    }

    val javadocJar = tasks.register<Jar>("javadocJar") {
        dependsOn(JavaPlugin.JAVADOC_TASK_NAME)
        archiveClassifier.set("javadoc")
        from(tasks.javadoc)
    }

    artifacts.add("archives", sourcesJar)
    artifacts.add("archives", javadocJar)

    if (project.hasProperty("sonatypeUsername")) {

        publishing {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])
                    artifact(tasks["sourcesJar"])
                    artifact(tasks["javadocJar"])
                    pom {
                        url.set("http://zulia.io")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }
                        developers {
                            developer {
                                id.set("mdavis")
                                name.set("Matt Davis")
                                email.set("matt.davis@ascend-tech.us")
                                organization.set("Ascendant Software Technology, LLC")
                                organizationUrl.set("http://www.ascend-tech.us")
                            }
                            developer {
                                id.set("payam.meyer")
                                name.set("Payam Mayer")
                                email.set("payam.meyer@ascend-tech.us")
                                organization.set("Ascendant Software Technology, LLC")
                                organizationUrl.set("http://www.ascend-tech.us")
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
                repositories {
                    maven {
                        val sonatypeUsername: String by project
                        val sonatypePassword: String by project

                        val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                        val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots/"
                        url = uri(if (!project.version.toString().contains("rc")) releasesRepoUrl else snapshotsRepoUrl)
                        credentials {
                            username = sonatypeUsername
                            password = sonatypePassword
                        }
                    }
                }
            }

        }

        signing {
            sign(publishing.publications["mavenJava"])
        }
    }


    group = "io.zulia"

    repositories {
        mavenCentral()
    }

    dependencies {
        //testImplementation("org.testng:testng:6.14.3")
        testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        systemProperty("mongoTestConnection", "mongodb://127.0.0.1:27017")
        workingDir = file("build/")
        jvmArgs = listOf("-Xmx1500m")

    }

}
