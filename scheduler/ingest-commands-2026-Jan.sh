#!/bin/bash
# Auto-generated ingest commands for Jan 2026
# Generated on Wed Jan 14 08:24:50 MST 2026

set -euo pipefail

I3_HOME="/Users/scott/dpla/code/ingestion3"
cd "$I3_HOME"


# Ingest: Harvard Library
echo "Starting ingest for Harvard Library..."
./ingest.sh harvard
if [ $? -eq 0 ]; then
    echo "✓ Harvard Library ingest completed successfully"
else
    echo "✗ Harvard Library ingest failed"
    exit 1
fi

# Ingest: National Archives and Records Administration
echo "Starting ingest for National Archives and Records Administration..."
./ingest.sh nara
if [ $? -eq 0 ]; then
    echo "✓ National Archives and Records Administration ingest completed successfully"
else
    echo "✗ National Archives and Records Administration ingest failed"
    exit 1
fi

# Ingest: Plains to Peaks
echo "Starting ingest for Plains to Peaks..."
# Scheduled for last week of month
./ingest.sh p2p
if [ $? -eq 0 ]; then
    echo "✓ Plains to Peaks ingest completed successfully"
else
    echo "✗ Plains to Peaks ingest failed"
    exit 1
fi

# Ingest: NCDHC
echo "Starting ingest for NCDHC..."
./ingest.sh digitalnc
if [ $? -eq 0 ]; then
    echo "✓ NCDHC ingest completed successfully"
else
    echo "✗ NCDHC ingest failed"
    exit 1
fi

# Ingest: MDL
echo "Starting ingest for MDL..."
./ingest.sh minnesota
if [ $? -eq 0 ]; then
    echo "✓ MDL ingest completed successfully"
else
    echo "✗ MDL ingest failed"
    exit 1
fi

# Ingest: Illinois Digital Heritage
echo "Starting ingest for Illinois Digital Heritage..."
./ingest.sh il
if [ $? -eq 0 ]; then
    echo "✓ Illinois Digital Heritage ingest completed successfully"
else
    echo "✗ Illinois Digital Heritage ingest failed"
    exit 1
fi

# Ingest: Recollection Wisconsin
echo "Starting ingest for Recollection Wisconsin..."
./ingest.sh wisconsin
if [ $? -eq 0 ]; then
    echo "✓ Recollection Wisconsin ingest completed successfully"
else
    echo "✗ Recollection Wisconsin ingest failed"
    exit 1
fi

# Ingest: Digital Commonwealth
echo "Starting ingest for Digital Commonwealth..."
./ingest.sh maryland
if [ $? -eq 0 ]; then
    echo "✓ Digital Commonwealth ingest completed successfully"
else
    echo "✗ Digital Commonwealth ingest failed"
    exit 1
fi

# Ingest: Michigan Service Hub
echo "Starting ingest for Michigan Service Hub..."
./ingest.sh mi
if [ $? -eq 0 ]; then
    echo "✓ Michigan Service Hub ingest completed successfully"
else
    echo "✗ Michigan Service Hub ingest failed"
    exit 1
fi
