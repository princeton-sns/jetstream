import socket

servers = []
#for i in range(1, 51):
#  servers.append("node"+str(i)+".mpisws.vicci.org")
#  servers.append("node"+str(i)+".gt.vicci.org")

s_as_str = """node1.washington.vicci.org
node2.washington.vicci.org
node3.washington.vicci.org
node5.washington.vicci.org
node6.washington.vicci.org
node10.washington.vicci.org
node12.washington.vicci.org
node14.washington.vicci.org
node15.washington.vicci.org
node16.washington.vicci.org
node17.washington.vicci.org
node19.washington.vicci.org
node21.washington.vicci.org
node22.washington.vicci.org
node23.washington.vicci.org
node25.washington.vicci.org
node26.washington.vicci.org
node27.washington.vicci.org
node28.washington.vicci.org
node29.washington.vicci.org
node50.washington.vicci.org
node51.washington.vicci.org
node52.washington.vicci.org
node53.washington.vicci.org
node54.washington.vicci.org
node55.washington.vicci.org
node56.washington.vicci.org
node57.washington.vicci.org
node58.washington.vicci.org
node59.washington.vicci.org
node61.washington.vicci.org
node65.washington.vicci.org
node68.washington.vicci.org
node69.washington.vicci.org
node70.washington.vicci.org
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
