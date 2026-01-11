# CSE535 F25 – Project 3: Paxos Sharded Bank

A local demo of a sharded, Paxos-based replicated bank with ten client workers and nine node processes organized into three clusters.
 
## Prerequisites
- **Java 21** (the Gradle wrapper will handle Gradle itself)
- **macOS/Linux shell** (commands below use `./gradlew`)
- **Ports 51051–51059** available on `127.0.0.1`

## Build
```bash
./gradlew clean build
```

## Run the nodes
From the repository root you can either run all nine nodes in one process or in separate terminals.

### Option A: run all nine nodes together (easiest)

```bash
./gradlew :node:runNodes
```

This starts nodes `n1`–`n9` on `127.0.0.1:51051`–`51059`.

### Option B: run each node in its own terminal

Open nine separate terminals, one per node, and run the following from the repository root:

- Terminal 1:
```bash
./gradlew :node:runNode1
```
- Terminal 2:
```bash
./gradlew :node:runNode2
```
- Terminal 3:
```bash
./gradlew :node:runNode3
```
- Terminal 4:
```bash
./gradlew :node:runNode4
```
- Terminal 5:
```bash
./gradlew :node:runNode5
```
- Terminal 6:
```bash
./gradlew :node:runNode6
```
- Terminal 7:
```bash
./gradlew :node:runNode7
```
- Terminal 8:
```bash
./gradlew :node:runNode8
```
- Terminal 9:
```bash
./gradlew :node:runNode9
```

These start nodes `n1`–`n9` on `127.0.0.1:51051`–`51059`.

## Run the client
Open a new terminal and run:
```bash
./gradlew :client:runClient
```
This launches a single client process that spawns 10 client workers (A–J) and executes CSV-defined test sets.

## Advancing between sets
When a set finishes you will see a prompt like:
```
Set N complete. Press Enter to continue...
client>
```
Press **Enter** (on a blank line at the `client>` prompt) to proceed to the next set.

## Client console commands
You can inspect cluster state any time during or after execution:
- **log** – Print transaction log from all nodes
- **db** – Print database/balances from all nodes
- **status <seq>** – Print status of a sequence number across all nodes
- **view** – Print view history (leader elections)
- **verify** – Verify database consistency across nodes
- **reshard plan [n] [tol] [seeds]** – Generate a resharding plan from recent history (writes `client/shard-map.new.json`)
- **reshard apply** – Promote the latest plan and reset nodes/client to use the new shard map
- **help** – Show help
- **quit** – Exit the client

Note: These inspection commands work even when nodes are temporarily inactive/frozen between sets.

## Resharding workflow (Project 3)

Resharding moves accounts between clusters to reduce cross-shard transactions while keeping shards balanced.

High-level flow:

1. **Run workload and collect history**  
   Transactions are appended to `client/history.jsonl` during normal execution.
2. **Plan** (from the client console, with nodes running and no other activity):  
   ```
   client> reshard plan [n] [tol] [seeds]
   ```
   - `n` (default `100000`): number of recent transactions to consider
   - `tol` (default `0.05`): balance tolerance (0–1)
   - `seeds` (default `10`): multi-start runs for the partitioner
   This produces `client/shard-map.new.json` describing the new mapping.
3. **Stop client and nodes** to go offline for migration (no traffic during migration).
4. **Run the offline migrator** (from the repository root):  
   ```bash
   ./gradlew :node:runOfflineMigrator \
       -PmigrNew=client/shard-map.new.json \
       -PmigrOld=client/shard-map.json
   ```
   If you omit the `-P` flags, these defaults are used.
   The migrator reads the old and new shard maps and moves account balances between node databases.
5. **Restart the nodes** using either `:node:runNodes` or the per-node tasks.  
   Do not run any transactions yet.
6. **Restart the client**:
   ```bash
   ./gradlew :client:runClient
   ```
7. **Apply the new mapping** from the client console:
   ```
   client> reshard apply
   ```
   This promotes `shard-map.new.json` to the live `shard-map.json` (in the appropriate `client/` path) and sends a reset RPC to all nodes so they reload the shard map and reseed according to the new ownership. The client also reloads its shard map so routing matches the servers.

After `reshard apply` completes, new transactions will be routed according to the resharded assignment.

## Customization (optional)
- Change CSV file for the client:
```bash
./gradlew :client:runClient -PpaxosClientCsv=path/to/your.csv
```
- Run nodes against a different host (defaults to 127.0.0.1):
```bash
./gradlew :node:runNode1 -PpaxosNodeHost=192.168.1.100
```
- Override client node targets explicitly (all nine nodes):
```bash
./gradlew :client:runClient \
  -PpaxosNodes="n1=127.0.0.1:51051,n2=127.0.0.1:51052,n3=127.0.0.1:51053,\
               n4=127.0.0.1:51054,n5=127.0.0.1:51055,n6=127.0.0.1:51056,\
               n7=127.0.0.1:51057,n8=127.0.0.1:51058,n9=127.0.0.1:51059"
```

## Stopping
- **Client**: type `quit` at `client>` or press Ctrl+C.
- **Nodes**: press Ctrl+C in each node terminal.

## Troubleshooting
- **Port in use**: ensure ports 51051–51055 are free or adjust ports in Gradle files.
- **Java version**: verify `java -version` reports 21.
