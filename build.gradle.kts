// See https://gradle.org and https://github.com/gradle/kotlin-dsl

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // Xodus
    compile("org.jetbrains.xodus:xodus-openAPI:1.2.2")
    compile("org.jetbrains.xodus:xodus-environment:1.2.2")

    // JUnit test framework
    testCompile("junit:junit:4.12")
}

val run by tasks.getting(JavaExec::class) {
    standardInput = System.`in`
}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.KVClient"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m", "-Xverify:none")
}
