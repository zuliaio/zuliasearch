import io.zulia.task.PicoCLITask

plugins {
    application
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


val picoCliZuliaAutoComplete = tasks.register<PicoCLITask>("autocompleteZulia") {
    completionScript = layout.buildDirectory.file("autocomplete/zulia.sh")
    driverClass = "io.zulia.tools.cmd.Zulia"
}


val picoCliZuliaAdminAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaAdmin") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliaadmin.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaAdmin"
}


val picoCliZuliaDumpAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaDump") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliadump.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaDump"
}


val picoCliZuliaRestoreAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaRestore") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliarestore.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaRestore"
}

val picoCliZuliaImportAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaImport") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliaimport.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaImport"
}

val picoCliZuliaExportAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaExport") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliaexport.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaExport"
}

val picoCliZuliaTestAutoComplete = tasks.register<PicoCLITask>("autocompleteZuliaTest") {
    completionScript = layout.buildDirectory.file("autocomplete/zuliatest.sh")
    driverClass = "io.zulia.tools.cmd.ZuliaTest"
}

project.tasks.withType(PicoCLITask::class.java).configureEach {
    runtimeClasspath.from(project.configurations.runtimeClasspath, sourceSets.main.get().output)
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

            from(picoCliZuliaAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaAdminAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaDumpAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaRestoreAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaImportAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaExportAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }
            from(picoCliZuliaTestAutoComplete.flatMap { it.completionScript }) {
                into("bin/autocomplete")
            }



            filePermissions {
                unix("777")
            }
            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        }

    }
}



