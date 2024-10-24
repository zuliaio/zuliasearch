plugins {
    java
    idea
    signing
    `maven-publish`
    alias(libs.plugins.reckon)
}

reckon {
    setDefaultInferredScope("patch")
    setScopeCalc(calcScopeFromProp())
    snapshots()
    stages("beta", "final")
    setStageCalc(calcStageFromProp())
}

allprojects {
    group = "io.zulia"
}
apply {
    from("javacc.gradle")
}

val Project.libs by lazy {
    the<org.gradle.accessors.dm.LibrariesForLibs>()
}

defaultTasks("build")
subprojects {

    apply(plugin = "java")
    apply(plugin = "idea")
    apply(plugin = "signing")
    apply(plugin = "maven-publish")

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
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

    repositories {
        mavenCentral()
    }


    dependencies {
        testImplementation(platform(libs.junit.bom))
        testImplementation(libs.jupiter.api)
        testImplementation(libs.jupiter.params)
        testRuntimeOnly(libs.jupiter.engine)
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
