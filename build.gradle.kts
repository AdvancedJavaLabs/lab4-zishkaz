plugins {
    kotlin("jvm") version "2.0.21"
}

group = "org.itmo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-client:3.4.1")
    implementation("org.apache.logging.log4j:log4j-bom:2.24.3")
    implementation("org.apache.logging.log4j:log4j-api:2.24.3")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.24.3")
    runtimeOnly("org.apache.logging.log4j:log4j-1.2-api:2.24.3")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}


val fatJar = task("fatJar", type = Jar::class) {
    archiveBaseName = "${project.name}-fat"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Implementation-Version"] = version
        attributes["Main-Class"] = "org.itmo.Main"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get() as CopySpec)
}

//tasks {
//    "build" {
//        dependsOn(fatJar)
//    }
//}

tasks.withType<org.gradle.jvm.tasks.Jar> {
    exclude("META-INF/BC1024KE.RSA", "META-INF/BC1024KE.SF", "META-INF/BC1024KE.DSA")
    exclude("META-INF/BC2048KE.RSA", "META-INF/BC2048KE.SF", "META-INF/BC2048KE.DSA")
}
