plugins {
    application
    `java-library`
}

description = "Zulia Tools"

dependencies {
    annotationProcessor(libs.picocli.codegen)
    implementation(project(":zulia-client"))
    implementation(project(":zulia-cmd-shared"))
    implementation(project(":zulia-testing"))
    implementation(project(":zulia-data"))
    implementation(libs.snake.yaml)
    implementation(libs.picocli.base)
    implementation(libs.commons.compress)
}


val zuliaScriptTask = tasks.getByName<CreateStartScripts>("startScripts")
zuliaScriptTask.applicationName = "zulia"
zuliaScriptTask.mainClass.set("io.zulia.tools.cmd.Zulia")


val zuliaAdminScriptTask = tasks.register<CreateStartScripts>("createZuliaAdminScript") {
    applicationName = "zuliaadmin"
    mainClass.set("io.zulia.tools.cmd.ZuliaAdmin")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}


val zuliaDumpScriptTask = tasks.register<CreateStartScripts>("createZuliaDumpScript") {
    applicationName = "zuliadump"
    mainClass.set("io.zulia.tools.cmd.ZuliaDump")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}

val zuliaRestoreScriptTask = tasks.register<CreateStartScripts>("createZuliaRestoreScript") {
    applicationName = "zuliarestore"
    mainClass.set("io.zulia.tools.cmd.ZuliaRestore")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}

val zuliaExportScriptTask = tasks.register<CreateStartScripts>("createZuliaExportScript") {
    applicationName = "zuliaexport"
    mainClass.set("io.zulia.tools.cmd.ZuliaExport")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}

val zuliaImportScriptTask = tasks.register<CreateStartScripts>("createZuliaImportScript") {
    applicationName = "zuliaimport"
    mainClass.set("io.zulia.tools.cmd.ZuliaImport")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}

val zuliaTestScriptTask = tasks.register<CreateStartScripts>("createZuliaTestScript") {
    applicationName = "zuliatest"
    mainClass.set("io.zulia.tools.cmd.ZuliaTest")
    outputDir = zuliaScriptTask.outputDir
    classpath = zuliaScriptTask.classpath
}

tasks.register("autocompleteDir") {
    doLast {
        mkdir("${layout.buildDirectory.get()}/autocomplete")
    }
}

task("picoCliZuliaAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zulia.sh",
        "io.zulia.tools.cmd.Zulia"
    )
}


task("picoCliZuliaAdminAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaadmin.sh",
        "io.zulia.tools.cmd.ZuliaAdmin"
    )
}

task("picoCliZuliaDumpAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args =
        listOf(
            "--force",
            "--completionScript",
            "${layout.buildDirectory.get()}/autocomplete/zuliadump.sh",
            "io.zulia.tools.cmd.ZuliaDump"
        )
}



task("picoCliZuliaRestoreAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliarestore.sh",
        "io.zulia.tools.cmd.ZuliaRestore"
    )
}


task("picoCliZuliaImportAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaimport.sh",
        "io.zulia.tools.cmd.ZuliaImport"
    )
}

task("picoCliZuliaExportAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliaexport.sh",
        "io.zulia.tools.cmd.ZuliaExport"
    )
}


task("picoCliZuliaTestAutoComplete", JavaExec::class) {
    dependsOn("autocompleteDir")
    mainClass.set("picocli.AutoComplete")
    classpath = sourceSets["main"].runtimeClasspath
    args = listOf(
        "--force",
        "--completionScript",
        "${layout.buildDirectory.get()}/autocomplete/zuliatest.sh",
        "io.zulia.tools.cmd.ZuliaTest"
    )
}

tasks.withType<AbstractArchiveTask> {
    dependsOn(
        "picoCliZuliaAutoComplete",
        "picoCliZuliaAdminAutoComplete",
        "picoCliZuliaDumpAutoComplete",
        "picoCliZuliaRestoreAutoComplete",
        "picoCliZuliaImportAutoComplete",
        "picoCliZuliaExportAutoComplete",
        "picoCliZuliaTestAutoComplete"
    )
}


distributions {
    main {
        contents {
            from(zuliaAdminScriptTask) {
                into("bin")
            }
            from(zuliaDumpScriptTask) {
                into("bin")
            }
            from(zuliaRestoreScriptTask) {
                into("bin")
            }
            from(zuliaExportScriptTask) {
                into("bin")
            }
            from(zuliaImportScriptTask) {
                into("bin")
            }
            from(zuliaTestScriptTask) {
                into("bin")
            }
            from("${layout.buildDirectory.get()}/autocomplete/") {
                into("bin/autocomplete")
            }

            fileMode = 777
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}



