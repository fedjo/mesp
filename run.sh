#!/bin/bash

sudo systemctl daemon-reload
sudo systemctl restart agent
python3 py/metrics.py $1
