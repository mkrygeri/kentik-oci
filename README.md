# kentik-oci

This is POC python code that will subscribe to an OCI flowlog stream and send it to Kentik. It is currently recomended that this be run as a container for ease of use. This can be run as commandline script if the python3 requirements in requirements.txt are installed. All testing done on Ubuntu 22.04.

1. Copy config.example.yaml to config.yaml 
2. Populate all the config variables for Kentik and OCI
3. run python3 kentik-oci.py
