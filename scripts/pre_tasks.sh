#!/bin/bash

# Check if application dir exists and create if not present.
if [[ ! -d /opt/cogynt-workstation-ingest ]]; then
	mkdir -p /opt/cogynt-workstation-ingest
fi

# Make sure cogynt-otp start script is present
if [[ ! -f "/usr/local/bin/cogynt-workstation-ingest" ]]; then
	cat > /usr/local/bin/cogynt-workstation-ingest << EOF
#!/bin/bash
start=0
stop=0

while true; do
    case "\$1" in
        start )  shift; start=1;;
        stop  )  shift; stop=1;;
        * ) break;;
    esac
done

if [[ \$start -eq 1 ]]; then
        cd /opt/cogynt-workstation-ingest/bin
        PORT=4002 ./cogynt-workstation-ingest start
elif [[ \$stop -eq 1 ]]; then
        cd /opt/cogynt-workstation-ingest/bin
        PORT=4002 ./cogynt-workstation-ingest stop
else
        echo "Null params"
fi
EOF
	chown root:root /usr/local/bin/cogynt-workstation-ingest
	chmod 755 /usr/local/bin/cogynt-workstation-ingest
fi

# Make sure systemctl unit file is present
if [[ ! -f "/etc/systemd/system/cogynt-workstation-ingest.service" ]]; then
	cat > /etc/systemd/system/cogynt-workstation-ingest.service << EOF
[Unit]
Description=Runner for Cogynt-workstation-ingest OTP App
After=network.service postgresql-11.service

[Service]
Type=forking
User=cogility
Group=cogility
WorkingDirectory=/opt/cogynt-workstation-ingest
ExecStart=/usr/local/bin/cogynt-workstation-ingest start
ExecStop=/usr/local/bin/cogynt-workstation-ingest stop
RemainAfterExit=yes
Environment=LANG=en_US.UTF-8
SyslogIdentifier=cogynt-workstation-ingest
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
	chown root:root /etc/systemd/system/cogynt-workstation-ingest.service
	chmod 644 /etc/systemd/system/cogynt-workstation-ingest.service
	systemctl daemon-reload
fi

# Ensure perms on app dir
chown cogility:cogility /opt/cogynt-workstation-ingest

exit 0
