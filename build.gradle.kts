import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "com.twisp"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.grpc:grpc-netty-shaded:1.76.0")
    implementation("io.grpc:grpc-protobuf:1.76.0")
    implementation("io.grpc:grpc-stub:1.76.0")
    implementation("io.grpc:grpc-kotlin-stub:1.4.1")
    implementation("com.google.protobuf:protobuf-java:4.33.0")
    implementation("com.google.protobuf:protobuf-kotlin:4.33.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
}

application {
    mainClass.set("com.twisp.kotlin101.MainKt")
}

val generatedJava = "build/generated/source/buf/main/java"
val generatedKotlin = "build/generated/source/buf/main/kotlin"

sourceSets {
    named("main") {
        java.srcDirs(generatedJava, generatedKotlin)
    }
}

kotlin.sourceSets.named("main") {
    kotlin.srcDirs(generatedKotlin, generatedJava)
}

val bufGenerate by tasks.registering(Exec::class) {
    group = "build"
    description = "Generate gRPC stubs from the Twisp API using buf.build"
    commandLine("buf", "generate", "buf.build/twisp/api")
    inputs.file("buf.gen.yaml")
    outputs.dir("build/generated/source/buf")
}

tasks.withType<KotlinCompile>().configureEach {
    dependsOn(bufGenerate)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_22)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.withType<JavaCompile>().configureEach {
    dependsOn(bufGenerate)
    options.release.set(22)
}

tasks.named("clean") {
    doFirst {
        delete("build/generated")
    }
}
