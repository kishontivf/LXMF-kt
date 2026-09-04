pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_PROJECT)
    repositories {
        // This is a fork, and it builds against OUR reticulum-kt fork rather than upstream's
        // published tags: the two diverge together, and a diagnostic added to one is used by the
        // other. `./gradlew publishToMavenLocal` in the reticulum-kt-kishontivf checkout is what
        // puts it here. Scoped to that group so nothing else resolves out of a directory whose
        // contents are whatever was last built.
        mavenLocal {
            content {
                includeGroupByRegex("com\\.github\\.torlando-tech.*")
            }
        }
        google()
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}

rootProject.name = "lxmf-kt"

include(":lxmf-core")
include(":lxmf-examples")

// :conformance-bridge applies the Shadow plugin, which is incompatible with
// Gradle 9. Columba consumes lxmf-kt via composite-build override on Gradle 9
// and would inherit the Shadow plugin transitively if this project were always
// included in the build. The conformance CI workflow sets INCLUDE_CONFORMANCE_BRIDGE=1
// on the build step that needs it (see .github/workflows/conformance.yml).
if (System.getenv("INCLUDE_CONFORMANCE_BRIDGE") != null) {
    include(":conformance-bridge")
}
