# Tempo 
Helm chart for Grafana Tempo in microservices mode

## Source Code

* <https://github.com/grafana/tempo>
* <https://grafana.com/docs/tempo/latest/>


| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicas | int | `1` | Pod Replicas |
| tempo.repository | string | `grafana/tempo` | Docker image repository for the tempo pod |
| tempo.tag | string | `latest` | Docker image tag for the tempo pod |
| tempo.pullPolicy | string | `IfNotPresent` | Docker image pull policy, enum: IfNotPresent, Always |
| tempoQuery.repository | string | `grafana/tempo` | Docker image repository for the query pod |
| tempoQuery.tag | string | `latest` | Docker image tag for the query pod |
| tempoQuery.pullPolicy | string | `IfNotPresent` | Docker image pull policy, enum: IfNotPresent, Always |
| persistence.enabled | bool | `false` | Should only be true if use local storage |
| persistence.accessModes | list | `ReadWriteOnce` | Persistence Access Mode |
| persistence.size | string | `10Gi` |  Persistence Size |
