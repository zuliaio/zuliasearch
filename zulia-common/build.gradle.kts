import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.protobuf

plugins {
    `java-library`
    alias(libs.plugins.protobuf)

}

defaultTasks("generateProto", "version", "build")

description = "Zulia Common"

dependencies {
    api(libs.annontations)
    api(libs.commons.pool2)
    api(libs.grpc.netty.shaded)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.javax.annotation)
    api(libs.protobuf.java.util)
    api(libs.simplemagic)

    protobuf(libs.sketches.java)

    api(projects.zuliaUtil)
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

tasks.register("version") {
    doLast {
        File("${project.layout.buildDirectory.get()}/classes/java/main/").mkdirs()
        File("${project.layout.buildDirectory.get()}/classes/java/main/version").writeText(project.version.toString())
    }
}

tasks.withType<JavaCompile> {
    dependsOn("version")
}

