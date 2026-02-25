import com.google.protobuf.gradle.id
//import com.google.protobuf.gradle.protobuf

plugins {
    alias(libs.plugins.protobuf)
}

defaultTasks("generateProto", "version", "build")

description = "Zulia Common"

dependencies {
    api(projects.zuliaUtil)
    api(libs.annontations)
    api(libs.grpc.netty.shaded)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.javax.annotation)
    api(libs.protobuf.java.util)
    api(libs.simplemagic)

    // commented out and DDSketch.proto is copied from the ddsktech repo, until ddsketch is patched to fix the poisoning
    // https://github.com/DataDog/sketches-java/issues/76
    //protobuf(libs.sketches.java)


}



protobuf {

    protoc {
        artifact = ("com.google.protobuf:protoc:" + libs.versions.protobuf.get())
    }
    plugins {
        id("grpc") {
            artifact = ("io.grpc:protoc-gen-grpc-java:" + libs.versions.grpc.get())
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
            }
        }
    }

}

abstract class WriteVersion : DefaultTask() {
    @get:OutputFile
    abstract val outputFile: RegularFileProperty

    @get:Input
    abstract val versionText: Property<String>

    @TaskAction
    fun write() {
        val f = outputFile.get().asFile
        f.parentFile.mkdirs()
        f.writeText(versionText.get())
    }
}

val generateVersion by tasks.registering(WriteVersion::class) {
    outputFile.set(layout.buildDirectory.file("version"))
    // Lazily read the project version
    versionText.set(providers.provider { project.version.toString() })
}

tasks.named<ProcessResources>("processResources") {
    from(generateVersion.flatMap { it.outputFile }) {
        into("/") // put version at the root of resources output
    }
}
