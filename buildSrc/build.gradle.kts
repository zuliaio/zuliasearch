plugins {
    `kotlin-dsl`
}

gradlePlugin {
    plugins {

    }
}

repositories {
    gradlePluginPortal()
}

kotlin {
    jvmToolchain {
        this.languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    //implementation("io.micronaut.gradle:micronaut-gradle-plugin:4.5.2")
    //implementation("com.gradleup.shadow:com.gradleup.shadow.gradle.plugin:9.1.0")
}