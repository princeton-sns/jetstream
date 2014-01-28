import socket

servers = []
#for i in range(1, 51):
#  servers.append("node"+str(i)+".mpisws.vicci.org")
#  servers.append("node"+str(i)+".gt.vicci.org")

s_as_str = """node6.gt.vicci.org
node19.gt.vicci.org
node15.gt.vicci.org
node8.gt.vicci.org
node5.gt.vicci.org
node18.gt.vicci.org
node14.gt.vicci.org
node9.gt.vicci.org
node35.gt.vicci.org
node3.gt.vicci.org
node1.gt.vicci.org
node10.gt.vicci.org
node11.gt.vicci.org
node12.gt.vicci.org
node14.gt.vicci.org
node15.gt.vicci.org
node16.gt.vicci.org
node17.gt.vicci.org
node18.gt.vicci.org
node19.gt.vicci.org
node20.gt.vicci.org
node22.gt.vicci.org
node24.gt.vicci.org
node28.gt.vicci.org
node29.gt.vicci.org
node30.gt.vicci.org
node31.gt.vicci.org
node32.gt.vicci.org
node33.gt.vicci.org
"""
#node31.washington.vicci.org
#node35.washington.vicci.org
#node36.washington.vicci.org
#node37.washington.vicci.org
#node38.washington.vicci.org
#node39.washington.vicci.org
#node41.washington.vicci.org
#node42.washington.vicci.org
#node43.washington.vicci.org
#node44.washington.vicci.org
#node45.washington.vicci.org
#node47.washington.vicci.org
#node48.washington.vicci.org
#node49.washington.vicci.org
#node67.washington.vicci.org
# 


servers = [s for s in s_as_str.splitlines()]

#servers=["node1.mpisws.vicci.org"]

for index, server in enumerate(servers):
  print server + " public_ip=" +socket.gethostbyname(server)+" server_index="+str(index)

print "---------------"
for server in servers:
  print server,
