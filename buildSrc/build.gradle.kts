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

}