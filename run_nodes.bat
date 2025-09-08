@echo off
REM Script para iniciar 3 nós no Windows
start cmd /k python app.py --node-id node1 --port 5001 --peers http://localhost:5002,http://localhost:5003
start cmd /k python app.py --node-id node2 --port 5002 --peers http://localhost:5001,http://localhost:5003
start cmd /k python app.py --node-id node3 --port 5003 --peers http://localhost:5001,http://localhost:5002
