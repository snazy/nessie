# "Higher level" integration tests

Integration tests that run against Nessie / Iceberg / Trino / Deltalake versions built by the
Nessie Integrations Test Suite. The tests run against any combination of versions of these projects.

## Why Java/Scala integration tests?

Because tests should be:
* runnable locally
* runnable in CI
* debuggable
* maintainable

Alternatives would have been to re-invent another custom integrations or regressions test suite,
demanding its own learning curve and becomes likely hard to maintain and probably very hard to
debug, hence Nessie uses "boring Java / Scala tests" built on "standard" test frameworks.

## Maven Wrapper et al

Necessary files (like `mvnw`) and folders (like `codestyle` and `.mvn`) are just symlinks to the
top-level files and folders.
