# swarm

Run stuff across multiple worker nodes in your local network.

No setup. No config. Simple.

This is work-in-progress.

## Goals

- simply start `swarmr` to make a machine a worker node
    - specify url of any other node to join the swarm
- each node has a web api
    - swarm management, discovery, ...
    - job management
- runners run jobs
    - a runner is a zip file
    - self-contained program or script
    - automatically shared between nodes
- jobs
    - a job starts a specified runner
    - specifies command line with args
    - jobs are enqueued within the swarm
    - nodes consume and process enqueued jobs

## Tasks

- [ ] maintaining distributed list of active nodes
- [ ] trivial status page (list of known nodes)
- [ ] distribution of runner packages
- [ ] work queue
- [ ] ...