AI Service Center App - Setup Instructions

Step 1: Run Docker secret setup:

A) Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

B) Run chmod +x setup-secrets.sh if the script does not have execution permissions.

C) Before running the script, gather all required details:

The script will first ask how many remote nodes you want to monitor, then prompt for each remote node:
- node name (what you want to call the node in the app)
- IP (the server's IP)
- username (the server username that has permission to access Docker over SSH)
- password

D) Execute ./setup-secrets.sh


Step 2: Deploy the Monitor app:

A) Navigate to the directory that contains the deploy.sh script and execute ./deploy.sh -f monitor

B) Wait for services to start.

C) Open the Monitor app: http://IP_ADDRESS:8081


Done.


Notes:
- The script creates a single Docker secret named nodes_config containing all remote node properties.
- Local node (server) is added automatically by backend through Docker socket.
- Ensure that port 22 (SSH) is open on all remote servers.


______________________________________________________________
______________________________________________________________

Post Deployment Updates

To add/remove/update remote nodes:
1. Remove the monitor Docker stack and wait a minute: 
docker stack rm monitor

2. Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

3. Re-run the Docker secret script:
./setup-secrets.sh

4. Redeploy:
./deploy.sh -f monitor

Done.
