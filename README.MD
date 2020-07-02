# Project 2 - SABD

## Installation

Go to scripts folder

```bash
sh compile.sh
```

## Local Usage

```bash
sh dataset_replay_and_results_storing.sh <queryNum> <numDays>
```
Launch Query1 or Query2 with parameter -> --numDays numDays

## Cluster Usage

```bash
sh start-env.sh
sh dataset_replay_and_results_storing.sh <queryNum> <numDays>
```
Upload project2-1.0.jar from scripts folder to flink container by UI from http://localhost:8081 

Launch Query1 or Query2 with parameters -> --hostname hostname --numDays numDays

```bash
sh stop-env.sh
```
