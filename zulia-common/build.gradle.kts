import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.protobuf

plugins {
    id("com.google.protobuf")
    `java-library`
}



defaultTasks("generateProto", "version", "build")

description = "Zulia Common"

val grpcVersion: String by project
val mongoDriverVersion: String by project
val protobufVersion: String by project
val gsonVersion: String by project

dependencies {
    api("io.grpc:grpc-netty-shaded:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("org.mongodb:bson:$mongoDriverVersion")
    api("com.google.protobuf:protobuf-java-util:$protobufVersion")
    api("org.mongodb:mongodb-driver-core:$mongoDriverVersion")
    api("org.apache.commons:commons-pool2:2.11.1")
    api("javax.annotation:javax.annotation-api:1.3.2")
    api("com.google.guava:guava:32.0.1-jre")
    api("com.j256.simplemagic:simplemagic:1.17")
    api("com.google.code.gson:gson:$gsonVersion")
    protobuf("com.datadoghq:sketches-java:0.8.2")
}



protobuf {

    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
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

