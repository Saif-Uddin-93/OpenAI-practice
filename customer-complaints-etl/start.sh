#!/bin/sh
pip3 install --no-cache-dir -r requirements.txt fastapi uvicorn[standard]
uvicorn src.app:app --host 0.0.0.0 --port 8000
