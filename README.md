# Aspen
General purpose distributed data platform for building higher-level distributed applications.

[Project Homepage](https://aspen-ddp.org)

# How to run the demo

## Setup

1. Run `sbt compile`
2. Copy t.template to t
3. Run sbt without any options to enter its cli
4. Run `export runtime:fullClasspath` to generate the full classpath needed to run the compiled code
5. exit sbt
5. Update the 't' file with the generated CLASSPATH and set the JAVA_HOME directory to your installed JDK location

## Execution
Run `./t bootstrap demo/bootstrap_config.yaml` to generate and initialize 3 nodes

In three separate terminals, execute the following:
`./t node demo/bootstrap_config.yaml demo/node_a.yaml`
`./t node demo/bootstrap_config.yaml demo/node_b.yaml`
`./t node demo/bootstrap_config.yaml demo/node_c.yaml`

In a fourth terminal run `./t nfs demo/bootstrap_config.yaml demo/log4j-conf.xml` to launch the AmoebaFS NFS server process.
Note that on MacOS you need to first run `sudo launchctl start com.apple.rpcbind` to allow the NFS server to
register with the local RPC daemon.

To attach a client to the NFS server, run the following on the client machine:
` umount -f /mnt; mount -v -t nfs4 -o "vers=4.1" <Server IP Address>:/ /mnt`





