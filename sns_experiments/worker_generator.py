import socket

servers = []
#for i in range(1, 51):
#  servers.append("node"+str(i)+".mpisws.vicci.org")
#  servers.append("node"+str(i)+".gt.vicci.org")

s_as_str = """
node1.washington.vicci.org
node10.washington.vicci.org
node12.washington.vicci.org
node15.washington.vicci.org
node16.washington.vicci.org
node17.washington.vicci.org
node19.washington.vicci.org
node2.washington.vicci.org
node21.washington.vicci.org
node22.washington.vicci.org
node23.washington.vicci.org
node25.washington.vicci.org
node26.washington.vicci.org
node28.washington.vicci.org
node5.washington.vicci.org
node50.washington.vicci.org
node51.washington.vicci.org
node53.washington.vicci.org
node55.washington.vicci.org
node56.washington.vicci.org
node57.washington.vicci.org
node59.washington.vicci.org
node6.washington.vicci.org
node61.washington.vicci.org
node68.washington.vicci.org
node69.washington.vicci.org
node1.gt.vicci.org
node10.gt.vicci.org
node11.gt.vicci.org
node12.gt.vicci.org
node14.gt.vicci.org
node14.gt.vicci.org
node15.gt.vicci.org
node15.gt.vicci.org
node16.gt.vicci.org
node17.gt.vicci.org
node18.gt.vicci.org
node18.gt.vicci.org
node19.gt.vicci.org
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
node35.gt.vicci.org
node6.gt.vicci.org
node1.mpisws.vicci.org
node2.mpisws.vicci.org
node3.mpisws.vicci.org
node4.mpisws.vicci.org
node5.mpisws.vicci.org
node6.mpisws.vicci.org
node7.mpisws.vicci.org
node8.mpisws.vicci.org
node9.mpisws.vicci.org
node11.mpisws.vicci.org
node15.mpisws.vicci.org
node17.mpisws.vicci.org
node19.mpisws.vicci.org
node20.mpisws.vicci.org
node21.mpisws.vicci.org
node22.mpisws.vicci.org
node23.mpisws.vicci.org
node24.mpisws.vicci.org
node26.mpisws.vicci.org
node27.mpisws.vicci.org
node28.mpisws.vicci.org
node30.mpisws.vicci.org
node31.mpisws.vicci.org
node33.mpisws.vicci.org
node34.mpisws.vicci.org
node35.mpisws.vicci.org
node38.mpisws.vicci.org
node39.mpisws.vicci.org
node41.mpisws.vicci.org
node42.mpisws.vicci.org
node43.mpisws.vicci.org
node45.mpisws.vicci.org
node47.mpisws.vicci.org
node49.mpisws.vicci.org
node50.mpisws.vicci.org
"""



servers = [s for s in s_as_str.splitlines()]

#servers=["node1.mpisws.vicci.org"]

for index, server in enumerate(servers):
  print server + " public_ip=" +socket.gethostbyname(server)+" server_index="+str(index)

print "---------------"
for server in servers:
  print server,
