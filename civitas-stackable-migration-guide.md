# Migration & Architecture Guide: Integrating Stackable Data Platform into CIVITAS/CORE

## 1. Executive Overview

This technical guide details the step-by-step architecture and deployment procedure for replacing standard **Strimzi Kafka** and **Apache NiFi** Helm deployments in **CIVITAS/CORE** with the **Stackable Data Platform (SDP)** operator suite.

### Key Architectural Shift

| Platform Component | Default CIVITAS/CORE Stack | Stackable Data Platform (SDP) Stack |
| :--- | :--- | :--- |
| **Operator Layer** | Strimzi Operator / NiFiKop / Helm | Stackable Operators (`kafka`, `nifi`, `zookeeper`, `secret`) |
| **Kafka Engine** | `kafka.strimzi.io/v1beta2` (`Kafka`) | `kafka.stackable.tech/v1alpha1` (`KafkaCluster`) |
| **NiFi Engine** | Standard Helm / NiFiKop CRD | `nifi.stackable.tech/v1alpha1` (`NifiCluster`) |
| **Configuration** | `values.yaml` overriding Helm charts | Declarative Kubernetes Custom Resource Definitions (CRDs) |
| **Authentication / TLS** | Custom secrets / cert-manager templates | Stackable `AuthenticationClass` & `SecretClass` objects |

---

## 2. Infrastructure Operator Layer (`helmfile-operators.yaml`)

Replace standard operators in the CIVITAS shared operator stack with Stackable operators.

```yaml
# deployment/helmfile-operators.yaml
repositories:
  - name: cloudnative-pg
    url: https://cloudnative-pg.github.io/charts
  - name: stackable
    url: https://repo.stackable.tech/repository/helm-stable/

environments:
  default:
    values:
      - operatorNamespace: civitas-operators

releases:
  # Standard PostgreSQL Operator for CIVITAS databases
  - name: cloudnative-pg
    namespace: {{ .Values.operatorNamespace }}
    chart: cloudnative-pg/cloudnative-pg
    version: 1.22.1

  # Stackable Operator Suite
  - name: stackable-commons-operator
    namespace: {{ .Values.operatorNamespace }}
    chart: stackable/commons-operator
    version: 24.7.0

  - name: stackable-secret-operator
    namespace: {{ .Values.operatorNamespace }}
    chart: stackable/secret-operator
    version: 24.7.0

  - name: stackable-zookeeper-operator
    namespace: {{ .Values.operatorNamespace }}
    chart: stackable/zookeeper-operator
    version: 24.7.0

  - name: stackable-kafka-operator
    namespace: {{ .Values.operatorNamespace }}
    chart: stackable/kafka-operator
    version: 24.7.0

  - name: stackable-nifi-operator
    namespace: {{ .Values.operatorNamespace }}
    chart: stackable/nifi-operator
    version: 24.7.0
```

---

## 3. Instance Layer Deployment (`helmfile-instance.yaml.gotmpl`)

In the instance layer, use the `bedag/raw` Helm chart to declare Stackable Custom Resources (ZooKeeper, Kafka, NiFi, and Security objects) before initializing CIVITAS microservices.

```yaml
# deployment/helmfile-instance.yaml.gotmpl
repositories:
  - name: bedag
    url: https://bedag.github.io/helm-charts/

environments:
  default:
    values:
      - instanceSlug: "demo-instance"
      - domain: "civitas.local"
      - keycloakUrl: "https://auth.civitas.local"

releases:
  # -------------------------------------------------------------
  # 1. STACKABLE DATA INFRASTRUCTURE & SECURITY CONFIGURATION
  # -------------------------------------------------------------
  - name: stackable-data-services
    namespace: {{ .Values.instanceSlug }}
    chart: bedag/raw
    version: 2.0.0
    values:
      - resources:
          # --- SecretClass for TLS Provisioning ---
          - apiVersion: secrets.stackable.tech/v1alpha1
            kind: SecretClass
            metadata:
              name: tls-cert-class
            spec:
              backend:
                autoTls:
                  ca:
                    secretConfig:
                      name: civitas-ca-secret
                      namespace: civitas-operators

          # --- AuthenticationClass for OIDC / Keycloak ---
          - apiVersion: authentication.stackable.tech/v1alpha1
            kind: AuthenticationClass
            metadata:
              name: keycloak-oidc
            spec:
              provider:
                oidc:
                  issuer: "{{ .Values.keycloakUrl }}/realms/civitas"
                  clientCredentials:
                    secretClass: keycloak-client-secret

          # --- ZooKeeper Cluster ---
          - apiVersion: zookeeper.stackable.tech/v1alpha1
            kind: ZookeeperCluster
            metadata:
              name: zookeeper
            spec:
              version: "3.8.3"
              servers:
                roleGroups:
                  default:
                    replicas: 3

          # --- Kafka Cluster (TLS Enabled) ---
          - apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: kafka
            spec:
              version: "3.6.1"
              zookeeperConfigMapName: zookeeper-zookeeper-zookeeper
              tls:
                secretClass: tls-cert-class
              brokers:
                roleGroups:
                  default:
                    replicas: 3

          # --- Apache NiFi Cluster (OIDC & TLS Enabled) ---
          - apiVersion: nifi.stackable.tech/v1alpha1
            kind: NifiCluster
            metadata:
              name: nifi
            spec:
              version: "1.23.2"
              zookeeperCluster:
                name: zookeeper
              authentication:
                - authenticationClass: keycloak-oidc
                  oidc:
                    clientSecret:
                      secretClass: keycloak-client-secret
              tls:
                secretClass: tls-cert-class
              nodes:
                roleGroups:
                  default:
                    replicas: 2

  # -------------------------------------------------------------
  # 2. CIVITAS/CORE INGESTION & PLATFORM SERVICES
  # -------------------------------------------------------------
  - name: civitas-ingestion-router
    namespace: {{ .Values.instanceSlug }}
    chart: ../charts/civitas-ingestion
    needs:
      - {{ .Values.instanceSlug }}/stackable-data-services
    values:
      - global:
          kafkaBootstrapServer: "kafka-broker.{{ .Values.instanceSlug }}.svc.cluster.local:9093"
          kafkaSecurityProtocol: "SSL"
          nifiEndpoint: "https://nifi-node-default.{{ .Values.instanceSlug }}.svc.cluster.local:8443"
```

---

## 4. OIDC & TLS Security Integration

### 4.1 OIDC Authentication via Keycloak

To connect Stackable NiFi to Keycloak, define an `AuthenticationClass` that provides single sign-on (SSO) capabilities across the data stack:

```yaml
apiVersion: authentication.stackable.tech/v1alpha1
kind: AuthenticationClass
metadata:
  name: keycloak-oidc
  namespace: demo-instance
spec:
  provider:
    oidc:
      issuer: "https://auth.civitas.local/realms/civitas"
      scopes:
        - openid
        - profile
        - email
        - groups
      principalClaim: "preferred_username"
      clientCredentials:
        secretClass: keycloak-client-secret
```

### 4.2 TLS Certificate Management via Secret Operator

Stackable manages internal wire encryption and service certificates via its `SecretClass` resource:

```yaml
apiVersion: secrets.stackable.tech/v1alpha1
kind: SecretClass
metadata:
  name: tls-cert-class
  namespace: demo-instance
spec:
  backend:
    autoTls:
      ca:
        secretConfig:
          name: civitas-ca-secret
          namespace: civitas-operators
```

---

## 5. Execution & Automation Workflow

Execute the deployment using standard Helmfile tooling in sequence:

```bash
# Step 1: Deploy Shared Stackable Operators
helmfile -f deployment/helmfile-operators.yaml sync

# Step 2: Create target instance namespace
kubectl create namespace demo-instance

# Step 3: Deploy Stackable Data Services and CIVITAS Platform
helmfile -f deployment/helmfile-instance.yaml.gotmpl sync   -e default   --state-values-set-string instanceSlug=demo-instance
```
