Service Center App - Setup Instructions

Step 1: New package placement

A) Place the monitor app package in the deployment directory (e.g. /home/ta9/app_version/stacks)

B) Extract the package
Run: tar -xvf monitor.tar

Step 2: Run Docker secret setup:

A) Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

B) Run chmod +x setup-secrets.sh if the script does not have execution permissions.

C) Before running the script, gather all required details:

The script will first ask how many remote nodes (servers) you want to monitor, then prompt for each remote node:
- node name (what you want to call the node in the app)
- IP (the server's IP)
- username (the server username that has permission to access Docker over SSH)
- password

D) Execute ./setup-secrets.sh and provide all the required details.

Step 3: Deploy the Monitor app:


makeeeeeeeeeeeee sure you have at leat 1 node that is a sworm manager


A) Navigate to the directory that contains the deploy.sh script (e.g. /home/ta9/app_version) and execute ./deploy.sh -f monitor

B) Wait for services to start.

C) Open the Monitor app: http://IP_ADDRESS:8081

Done.

Notes:
- The script creates a single Docker secret named nodes_config containing all remote node properties.
- Local node (server) is added automatically by backend through Docker socket.
- Ensure that port 22 (SSH) is open on all remote servers.
- Ensure that the Docker socket is available at /var/run/docker.sock (if it is located elsewhere, update the frontend service in the Docker Compose file to mount the correct host path).

______________________________________________________________
______________________________________________________________

Post Deployment Updates

To add/remove/update remote nodes:
A) Remove the monitor Docker stack and wait a minute: 
docker stack rm monitor

B) Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

C) Re-run the Docker secret script:
./setup-secrets.sh

D) Redeploy:
./deploy.sh -f monitor


