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
