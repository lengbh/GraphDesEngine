# GraphDesEngine

A Discrete-Event Simulation (DES) engine that converts system graph models into executable Digital Twin simulations.

Developed by Bohan Leng

---


## Overview
GraphDesEngine executes simulation of manufacturing systems modelled as labelled directed graphs. Vertices represent stations with service-time distributions and finite buffers; arcs represent transfers with transfer-time distributions. The engine uses SimPy to simulate trays/workpieces behaviours as they flow through the graph.

## Features
- JSON-defined labelled graphs (vertices & arcs) with parameterised time distributions
- Station buffers and service processes
- Transfer processes between stations
- Optional MES control mode via TCP communication
- CSV event logging for analysis

## Installation & Run
```shell
git clone https://github.com/lengbh/GraphDesEngine.git
cd GraphDesEngine
python -m venv venv
source venv/bin/activate 
pip install -r requirements.txt
python sim_engine.py
```
>Note: for MES control mode ON, the external MES Server is required. See the [MES Server repository](https://github.com/lengbh/ReconfigManus)'s [release page](https://github.com/lengbh/ReconfigManus/releases/tag/v1.0) for MESServer binaries and essential libraries to be copied to python.

## File structure

- `sim_engine.py` — Main simulation driver and event logger
- `labelled_graph.py` — Graph model (Vertex, Arc, LabelledGraph), JSON loading
- `random_util.py` — RandomFactory for distribution sampling
- `SystemGraphs/` — Example graph JSON files
- `EventLogs/` — Example/produced CSV event logs
- `requirements.txt `— Python dependencies

The following files are related to TCP Communication with external MES Server, relying on the TCP library binaries (to be copied from [MES Server release page](https://github.com/lengbh/ReconfigManus/releases/tag/v1.0)):

- `tcp_client.py` — Base python TCP client using [`TCPConn`](https://github.com/lengbh/TCPConn)
- `station_tcp_client.py `— Station TCP client for emulating query to and response from MES Server
- `station_protocol.py` — Query/response message protocol (for MES/TCP control mode)

## System graph model input

The modelling of manufacturing systems is described in a JSON file as labelled directed graphs. Three examples are given in `SystemGraphs/`.

To accept control from external MES Server, the same graph JSON file which `sim_engine.py` loads should also be copied to and loaded by MES Server. 

## Event logs output

Events are appended to a CSV file configured by `OUT_LOG_CSV_FILE` in `sim_engine.py`. Event types include:

- `injected`: A tray is created and sent to a station’s input buffer
- `enqueued` / `dequeued`: A tray has been put into a station’s buffer and taken out for service
- `service_start` / `service_end`: Service start and completion at stations
- `transfer_start` / `transfer_end`: Movement over an arc between stations
- `tray_completed`: A tray has completed (e.g., no next station)

