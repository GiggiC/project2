# Project 2 - SABD - Corsi Luigi

## Installation

Go to scripts folder

```bash
sh compile.sh
```

## Local Usage

To replay dataset and write results:

```bash
sh dataset_replay_and_results_storing.sh <queryNum> <numDays>
```
Launch Query1 or Query2 with parameter -> --numDays numDays
<br>It's possible to configure other parameters setted by default:
<br><br>--hostname 
<br>--inputPort
<br>--exportPort

## Cluster Usage

To start environment, replay dataset and write results:

```bash
sh start-env.sh
sh dataset_replay_and_results_storing.sh <queryNum> <numDays>
```
Upload project2-1.0.jar from scripts folder to flink container by UI from http://localhost:8081 

Launch Query1 or Query2 with parameter -> --hostname hostname --numDays numDays
<br>It's possible to configure other parameters setted by default: 
<br><br>--inputPort
<br>--exportPort
<br>
<br>To stop environment:

```bash
sh stop-env.sh
```
## Parallelism
To get orderd csv lines set parallelism to 1