import org.ajoberstar.reckon.gradle.ReckonExtension

plugins {
	java
	maven
	idea
	id("org.ajoberstar.reckon") version "0.9.0"
	id("com.google.protobuf") version "0.8.8" apply false
}

configure<ReckonExtension> {
	scopeFromProp()
	stageFromProp("rc", "final")
}

defaultTasks("build")
subprojects {

	apply(plugin = "java")
	apply(plugin = "maven")
	apply(plugin = "idea")

	group = "io.zulia"

	repositories {
		mavenCentral()
	}

	dependencies {
		testCompile("org.testng:testng:6.14.3")
	}

	tasks.withType<Test> {
		useTestNG()
		systemProperty("mongoTestConnection", "mongodb://127.0.0.1:27017")
		workingDir = file("build/")
		jvmArgs = listOf("-Xmx1500m")

	}

	val sourcesJar = tasks.register<Jar>("sourcesJar") {
		archiveClassifier.set("sources")
		from(sourceSets.getByName("main").allSource)
	}


	artifacts.add("archives", sourcesJar)

}
