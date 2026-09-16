# CIVITAS/CORE with Stackable Kafka and NiFi

This project installs the CIVITAS/CORE platform from
[`civitas-core-deployment`](../civitas-core-deployment) on Kubernetes. Two parts are
swapped for versions from the [Stackable Data Platform](https://docs.stackable.tech/) 26.7:

| Part | Before | Now |
|---|---|---|
| Kafka | Strimzi, Kafka 4.1.0 | Stackable, Kafka 4.2.1 |
| NiFi | NiFi Helm chart, NiFi 2.9.0 | Stackable, NiFi 2.9.0 |

Everything else (Keycloak, APISIX, PostgreSQL, FROST, portal and more) comes unchanged
from civitas-core-deployment. There is no ZooKeeper. Kafka runs in KRaft mode and NiFi
keeps its cluster state in Kubernetes.

## How it works

`just setup` downloads civitas-core-deployment (version `v2.0-rc2`) into the hidden folder
`.civitas-core-deployment/`. civitas-core-deployment looks for extra components in its
folder `deployment/addons/`. If it finds one there, it uses it instead of its own. This
project brings such a folder and links it in with `just link`.

```
civitas-stackable-demo/
├── justfile                        all commands
├── .civitas-core-deployment/       downloaded by `just setup` (not in git)
├── helmfile-operators.yaml.gotmpl  installs the operators (once per cluster)
├── helmfile-instance.yaml.gotmpl   installs the platform
├── values/default-instance.yaml    your settings
├── deployment/addons/
│   ├── stackable/                  the 5 Stackable operators
│   ├── kafka/                      Kafka and kafka-ui
│   └── nifi/                       NiFi, Keycloak login and web page
├── clusters/                       create a kind or k3d cluster
├── demo/                           demo pipeline (`just demo`)
├── patches/                        one small change for civitas-core-deployment
└── tests/                          check scripts
```

civitas-core-deployment needs one small change, so config-adapter can find the new
NiFi. It is in `patches/` and `just setup` adds it.

## What you need

- `kubectl`, `helm` with the `helm-diff` plugin, `helmfile`, `yq`, `envsubst`, [`just`](https://github.com/casey/just)
- `git`, `kind` or `k3d`, and Docker (`jq` and `python3` for the demo)
- About 8 CPU cores and 20 GB memory. The first install takes 20 to 30 minutes.

```bash
brew install helmfile yq kind
helm plugin install https://github.com/databus23/helm-diff   # helm 4 needs --verify=false
```

## Start

```bash
just setup                # once: download civitas-core-deployment and patch it
just check-tools          # anything missing?
just deploy               # cluster, operators and platform
just smoke-test           # is everything working?
```

Already have your own civitas-core-deployment checkout? Set `CIVITAS_CORE_DEPLOYMENT` to
its path before `just setup`. It must be close to `v2.0-rc2`, or the patch may not fit.

`just deploy` creates a kind cluster. Use `CLUSTER=k3d just deploy` for k3d, or
`CLUSTER=none just deploy` for a cluster you already have. Your own cluster needs
ingress-nginx, cert-manager with a `selfsigned-ca` issuer and DNS for `*.civitas.test`
(`clusters/common/bootstrap.sh` installs the first two).

## Open NiFi in the browser

1. `just add-hosts` points the `civitas.test` names to your computer (asks for sudo).
2. Make port 443 reach the cluster:
   - kind cluster made by `just deploy`: nothing to do.
   - any other cluster: keep `just port-forward 443` running (asks for sudo).
3. `just create-admin-user`, then `just credentials` shows the password.
4. Open https://nifi.civitas.test/nifi and log in as `admin@civitas.test`.

The browser warns about the certificate. Accept it, or import
`civitas-core-deployment/dev-deployment/.ssl/civitas.crt`. Logins only work on port 443,
because Keycloak does not accept other ports.

## Commands

| Command | What it does |
|---|---|
| `just setup` | Download and patch civitas-core-deployment (once) |
| `just deploy` | Everything: cluster, operators, platform |
| `just operators` | Install or update the operators |
| `just instance` | Install or update the platform |
| `just sync-component nifi` | Install or update one part (`kafka`, `nifi`, `keycloak`, ...) |
| `just status` | Show what is running |
| `just credentials` | Show logins |
| `just create-admin-user` | Set a password for `admin@civitas.test` |
| `just add-hosts` / `just port-forward 443` | Browser access |
| `just kafka-ui` | Open kafka-ui on http://localhost:8080 |
| `just template` / `just diff` | Show the Kubernetes files or the changes, without installing |
| `just demo` / `just demo-down` | Demo pipeline in the portal (see below) |
| `just smoke-test` | Run all checks (single checks: `just test-render`, `test-kafka`, `test-nifi`, ...) |
| `just destroy` | Delete the cluster |

Want to run helmfile yourself? Set the path first:

```bash
export CIVITAS_CORE_DEPLOYMENT=$(realpath .civitas-core-deployment)
helmfile -f helmfile-instance.yaml.gotmpl -e local -l component=nifi apply
```

## Share it

Send the folder without `.civitas-core-deployment/` and `.git`. The easiest way:

```bash
git archive --format=zip -o ../civitas-stackable-demo.zip HEAD
```

The other person unzips it, installs the tools and runs `just setup` and `just deploy`.

## Settings

Change them in `values/default-instance.yaml`:

| Setting | Default | Meaning |
|---|---|---|
| `global.domain` | `civitas.test` | Web addresses like `nifi.civitas.test` |
| `global.instanceSlug` | `dev` | Kubernetes namespace and Keycloak realm |
| `global.initialUserEmail` | `admin@civitas.test` | Admin user for NiFi |
| `kafka.cluster.productVersion` | `4.2.1` | Kafka version (Stackable also has `3.9.2`, not tested here) |
| `nifi.nifi.productVersion` | `2.9.0` | NiFi version |
| `nifi.nifi.ingress.enabled` | `true` | NiFi web page on/off |

More settings (size, disks, replicas) are in `deployment/addons/*/charts/*/values.yaml`.
Operator versions are in `deployment/addons/stackable/charts.yaml`. You can add your own
values file with `EXTRA_VALUES=my-values.yaml just instance`.

## Useful commands

Kafka:

```bash
kubectl -n dev exec kafka-cluster-broker-default-0 -c kafka -- \
  /stackable/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server kafka-cluster-broker-default-bootstrap:9092
```

kafka-ui (web page for Kafka): run `just kafka-ui`, keep it running and open
http://localhost:8080. It needs no login and no port 443.

NiFi status and logs:

```bash
kubectl -n dev get nificluster nifi-nifi
kubectl -n dev logs nifi-nifi-node-default-0 -c nifi
```

## Demo pipeline

`just demo` shows Kafka and NiFi at work inside CIVITAS. You need `just create-admin-user`
and port 443 first (see "Open NiFi in the browser").

It starts a small MQTT broker with a sender that posts a fake reading every 5 seconds. Then
it creates a data pool, a data structure, an MQTT data source and the dataset
"Stackable Demo" with a pipeline in the portal, and releases the dataset. config-adapter
gets the job over Kafka and builds the flow in NiFi. The readings go MQTT, NiFi, PostGIS.

Where to look:

- Portal: https://portal.civitas.test, "Unsere Daten", "Datensätze", "Stackable Demo". The
  pipeline shows its status there (NiFi reports it back over Kafka).
- NiFi: https://nifi.civitas.test/nifi, the process group `pipeline-...` with running processors.
- kafka-ui (`just kafka-ui`, then http://localhost:8080): the topics
  `de.civitascore.dataset.saga.trigger` (job for config-adapter), `de.civitascore.saga.result`
  (result) and `de.civitascore.pipeline.status` (pipeline status from NiFi).
- Data: `just demo-rows` shows the rows in PostGIS.

`just demo-down` takes the dataset back to draft (config-adapter removes the NiFi flow) and
removes the MQTT broker. Running `just demo` again brings it back. The entries in the portal
and the rows in PostGIS stay. Delete them in the portal if you do not need them anymore.

## End-to-end test

`just system-test` runs the "Hero Case" test of civitas-core-deployment in a container. It
creates a data structure, a data source and a dataset with two pipelines, using the portal
API and a real browser. You need `just create-admin-user` first and port 443 (see above).
The report ends up in `.civitas-core-deployment/tests/system/results/report.html`.

With civitas-core-deployment `v2.0-rc2` it does not pass all steps yet. This is not caused
by Stackable:

- Each run leaves its entries in the portal (names start with "Saga" or "Test"). Delete them
  in the portal if they get in the way.
- 34 steps pass. Then the test gets stuck on a "save before leaving?" dialog in the pipeline editor.
- If you release the dataset by hand, the whole saga runs over Kafka (FROST, APISIX, PostGIS,
  GeoServer). The NiFi step then stops in config-adapter before it talks to NiFi, because the
  test pipeline uses an old ID format (`urn:core:datastructure:...`).

## Problems

| Problem | Fix |
|---|---|
| `run 'just setup'` from check-tools | Run `just setup`. |
| `no matches for kind KafkaCluster` | Operators missing. Run `just operators`. |
| `requiredEnv CIVITAS_CORE_DEPLOYMENT` | Set `CIVITAS_CORE_DEPLOYMENT` (see Commands). |
| `Account is not fully set up` in Keycloak | Run `just create-admin-user`. |
| Login fails after the Keycloak page | Use port 443 (see "Open NiFi in the browser"). |
| NiFi says `No applicable policies` after changing the admin | NiFi only sets the admin on the first start. Delete it and its disks, then reinstall: `kubectl -n dev delete nificluster nifi-nifi`, `kubectl -n dev delete pvc -l app.kubernetes.io/instance=nifi-nifi`, `just sync-component nifi`. |
| Something can not connect | Check the NetworkPolicies in `deployment/addons/*/networkpolicies.yaml`. |

## Differences to civitas-core-deployment

- Linkerd (service mesh) and Kyverno (policies) are turned off. Their rules only know Strimzi.
- The NiFi web page is reachable at `https://nifi.civitas.test`. Before, it was internal only.
- The unused Kafka topic `kafkasql-journal` is gone. Topics are created when first used.
- Kafka and NiFi run with one server each.
