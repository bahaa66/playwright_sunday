#!/bin/bash
rm -rf /opt/cogynt-workstation-ingest/*
cd /opt/cogynt-workstation-ingest && find . -mindepth 1 -delete
exit 0
