import socket

servers = []
for i in range(1, 51):
  servers.append("node"+str(i)+".mpisws.vicci.org")
  servers.append("node"+str(i)+".gt.vicci.org")

#servers=["node1.mpisws.vicci.org"]

for index, server in enumerate(servers):
  print server + " public_ip=" +socket.gethostbyname(server)+" server_index="+str(index)

print "---------------"
for server in servers:
  print server,
