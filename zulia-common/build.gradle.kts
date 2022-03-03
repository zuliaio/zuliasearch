import com.google.protobuf.gradle.*

plugins {
    id("com.google.protobuf")
    `java-library`
}



defaultTasks("generateProto", "build")

description = "Zulia Common"


val grpcVersion: String by project
val mongoDriverVersion: String by project
val protobufVersion: String by project


dependencies {
    api("io.grpc:grpc-netty-shaded:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("org.mongodb:bson:$mongoDriverVersion")
    api("org.apache.commons:commons-pool2:2.7.0")
    api("javax.annotation:javax.annotation-api:1.3.2")
    api("com.google.guava:guava:30.0-jre")
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
                //"enable_deprecated=false"
            }
        }
    }

    generatedFilesBaseDir = "$projectDir/gen"
}

tasks {
    clean {
        delete("$projectDir/gen")
    }

}
tasks.register("version") {
    doLast {
        File("${project.buildDir}/classes/java/main/version").writeText(project.version.toString())
    }
}

tasks.withType<JavaCompile> {
    dependsOn("version")
}

