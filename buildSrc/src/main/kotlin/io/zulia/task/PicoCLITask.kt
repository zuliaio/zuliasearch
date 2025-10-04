package io.zulia.task

import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.*
import org.gradle.process.ExecOperations
import javax.inject.Inject

@CacheableTask
abstract class PicoCLITask : DefaultTask() {

    @get:Input
    abstract val driverClass: Property<String>

    @get:OutputFile
    abstract val completionScript: RegularFileProperty

    @get:Classpath
    abstract val runtimeClasspath: ConfigurableFileCollection

    @get:Inject
    protected abstract val execOps: ExecOperations

    @TaskAction
    fun generate() {
        val out = completionScript.get().asFile
        out.parentFile.mkdirs()
        
        execOps.javaexec {
            classpath = runtimeClasspath
            mainClass.set("picocli.AutoComplete")
            args(
                "--force",
                "--completionScript", completionScript.asFile.get().absolutePath,
                driverClass.get()
            )
        }
    }
}