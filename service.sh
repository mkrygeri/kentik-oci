systemctl stop kentik-oci.service
rm -f /usr/lib/systemd/system/kentik-oci.service
rm -rf /opt/kentik/kentik-oci/
cp ./kentik-oci.service /usr/lib/systemd/system/kentik-oci.service
mkdir -p /opt/kentik
mkdir -p /opt/kentik/kentik-oci/
cp ./kentik_oci_flow.py /opt/kentik/kentik-oci/
cp ./config.yaml /opt/kentik/kentik-oci/
cp ./kentik.env.example /etc/kentik.env
systemctl enable kentik-oci.service
systemctl start kentik-oci.service

