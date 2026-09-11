# Data platform: Kafka + HDFS + Hive 4.0.0 + Hue

## Layout
```
data-platform/
  docker-compose.yml
  hive/
    Dockerfile        # apache/hive:4.0.0 + Postgres JDBC driver
    conf/
      core-site.xml   # tells Hive where HDFS is
      hdfs-site.xml
  hue/
    hue.ini
```

## Run
```bash
docker compose up -d --build
docker compose logs -f hive-metastore hive-server
```

First boot order that actually matters: `postgres-metastore` up → `namenode`/`datanode` up →
`hive-metastore` runs `schematool -initSchema` against Postgres (this is automatic,
built into the official image's entrypoint) → `hive-server` connects to the metastore
over thrift → `hue` can finally reach `hive-server:10000`. That chain is why the first
`docker compose up` can take a minute or two before Hue actually works — give it time
before assuming something's broken.

## Verify each layer, in order
Don't jump straight to Hue — check the layer below it first, it saves you from
debugging the wrong service.

1. **HDFS is up:** open `http://localhost:9870` — you should see the NameNode
   overview page with 1 live datanode.
2. **Metastore is up:**
   ```bash
   docker compose logs hive-metastore | tail -30
   ```
   Look for `Starting Hive Metastore Server` with no exceptions after it.
3. **HiveServer2 is up and can reach the metastore:**
   ```bash
   docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/'
   ```
   If this connects and gives you a `0: jdbc:hive2://localhost:10000/>` prompt,
   Hive itself is fine and any remaining issue is in Hue's config, not the backend.
4. **Then** try Hue at `http://localhost:8888`.

## Things worth knowing about this setup

- **Hadoop is 3.2.1, not 3.4.0.** `bde2020/hadoop-namenode`/`datanode` — the
  maintainer never published a 3.4.0 tag, and hand-rolling namenode/datanode
  from the official `apache/hadoop` image means writing your own formatting
  and startup scripts (that image expects you to run `hdfs namenode -format`
  and manage config yourself, no env-var templating). Given you said you're
  lost, I went with what's proven to actually come up cleanly. If matching
  the exact version number matters for a deliverable, say so and I'll build
  the from-scratch `apache/hadoop:3.4.0` version — it's a bigger lift.
- **Hive is genuinely 4.0.0** — official `apache/hive` image, confirmed against
  current Apache docs. It doesn't ship a Postgres driver, hence the tiny
  `hive/Dockerfile` that just adds the jar.
- **`hive-metastore` and `hive-server` are the same image, different `SERVICE_NAME`.**
  That env var is what the official image's entrypoint reads to decide which
  process to actually start.
- **This is the same class of bug you hit last time:** if you ever add more
  `HIVE_*` env vars by hand instead of `SERVICE_OPTS`/`HIVE_CUSTOM_CONF_DIR`,
  they'll silently do nothing — the official image doesn't use the
  `HIVE_CORE_CONF_*`-style prefix scanning that bde2020's images do.
- If `hive-server` crash-loops before `hive-metastore` finishes its schema
  init, `restart: on-failure` will just retry it — check
  `docker compose ps` after a minute; both should show `Up`, not `Restarting`.
- Data persistence: HDFS data, the Postgres metastore DB, and the Hive
  warehouse dir are all named volumes now (`hadoop_namenode`, `pg_metastore_data`,
  `warehouse`) — `docker compose down` (without `-v`) keeps everything;
  add `-v` if you actually want a clean slate.