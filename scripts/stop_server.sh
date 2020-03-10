#!/bin/bash
if [[ -f "/etc/systemd/system/cogynt-workstation-ingest.service" ]]; then
        systemctl stop cogynt-workstation-ingest  &>/dev/null
fi
exit 0
