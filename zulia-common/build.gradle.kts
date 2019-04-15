import com.google.protobuf.gradle.*
plugins {
    id("com.google.protobuf")
}



defaultTasks("generateProto", "build")

description = "Zulia Common"

val grpcVersion = "1.20.0"

dependencies {
    compile("io.grpc:grpc-netty-shaded:${grpcVersion}")
    compile("io.grpc:grpc-protobuf:${grpcVersion}")
    compile("io.grpc:grpc-stub:${grpcVersion}")
    compile("org.mongodb:bson:3.10.1")
    compile("org.apache.commons:commons-pool2:2.6.0")
    compile("javax.annotation:javax.annotation-api:1.3.2")
}



protobuf {

    protoc {
        artifact = "com.google.protobuf:protoc:3.6.1"
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


