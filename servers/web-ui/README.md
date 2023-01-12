# Nessie Web UI

## Continuous Development 

1. Start Quarkus with the appropriate CORS configuration:
   ```bash
   java "-Dquarkus.http.cors.origins=http://127.0.0.1:8080,http://localhost:8080" -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
   ```
2. Start Webpack serve 
   ```bash
   ./gradlew :nessie-web-ui:browserDevelopmentRun --continuous
   ```
3. Open browser at `http://127.0.0.1:8080/?uri=http://127.0.0.1:19120/api/v2/`
   (trailing slash is mandatory)

## Basic layout concept

```
+-------------------------------------------------------------------------------------------------+
| Header bar                                                                            <actions> |
+-----------+-------------+-----------------------------------------------------------------------+
| <actions> | <actions>   | <actions>                                                             |
| branches  | namespaces  | content list and details                                              |
| and       |             |                                                                       |
| tags      |             |                                                                       |
|           |             |                                                                       |
|           |             |                                                                       |
|           |             |                                                                       |
|           |             |                                                                       |
|           |             |                                                                       |
|           |             |                                                                       |
|           +-------------+-----------------------------------------------------------------------+
|           | commits                                                                             |
|           |                                                                                     |
|           |                                                                                     |
|           |                                                                                     |
+-------------------------------------------------------------------------------------------------+
```
