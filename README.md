Service Center App - Setup Instructions


Step 1: New packages placement

A) Place the both monitor app packages in the deployment directory in one of the cluster Sworm managers (path for example: /home/ta9/app_version/stacks)

B) Extract the Docker images layers:
Run: docker load -i monitor-images.tar 

C) Extract the deployment package
Run: tar -xvf monitor.tar



Step 2: Run Docker secret setup:

A) Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

B) Run chmod +x setup-secrets.sh if the script does not have execution permissions.

C) Before running the script, gather all required details:

The script will first ask how many remote nodes (servers) you want to monitor, then prompt for each remote node:
- node name (what you want to call the node in the app)
- IP (the server's IP)
- username (the server username that has permission to use Docker)
- password (the server username password)

D) Execute ./setup-secrets.sh and provide all the required details.



Step 3: Deploy the Monitor app:

A) Navigate to the directory that contains the deploy.sh script (e.g. /home/ta9/app_version/stacks) and execute ./deploy.sh -f monitor

B) Wait for services to start, and open the Monitor app: http://IP_ADDRESS:8081


Done.



Notes:
- The script creates a single Docker secret named nodes_config containing all remote nodes properties.
- Local node (server) is added automatically.
- Ensure that port 22 (SSH) is open on all remote servers.
- Ensure that the Docker socket is available at /var/run/docker.sock (if it is located elsewhere, update the frontend service in the Docker Compose file to mount the correct host path).

______________________________________________________________
______________________________________________________________

Post Deployment Updates

To add/remove/update remote nodes:

A) Remove the monitor Docker Sworm stack and wait a minute: 
docker stack rm monitor

B) Remove the Docker Secret:
docker secret rm nodes_config

C) Navigate to the directory that contains setup-secrets.sh (monitor/setup-secrets.sh)

D) Re-run the Docker secret script:
./setup-secrets.sh

E) Redeploy:
./deploy.sh -f monitor


