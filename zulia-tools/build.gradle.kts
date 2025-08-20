plugins {
    application
    `java-library`
}

description = "Zulia Tools"

defaultTasks("build", "installDist")

dependencies {
    annotationProcessor(libs.picocli.codegen)
    implementation(projects.zuliaClient)
    implementation(projects.zuliaCmdShared)
    implementation(projects.zuliaTesting)
    implementation(projects.zuliaData)
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
    dependsOn(":zulia-common:version")
    doLast {
        mkdir("${layout.buildDirectory.get()}/autocomplete")
    }
}

tasks.register<JavaExec>("picoCliZuliaAutoComplete") {
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


tasks.register<JavaExec>("picoCliZuliaAdminAutoComplete") {
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

tasks.register<JavaExec>("picoCliZuliaDumpAutoComplete") {
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



tasks.register<JavaExec>("picoCliZuliaRestoreAutoComplete") {
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


tasks.register<JavaExec>("picoCliZuliaImportAutoComplete") {
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

tasks.register<JavaExec>("picoCliZuliaExportAutoComplete") {
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


tasks.register<JavaExec>("picoCliZuliaTestAutoComplete") {
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

            filePermissions {
                unix("777")
            }
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}



