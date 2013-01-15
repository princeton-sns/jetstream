import socket

servers=[
"node1.mpisws.vicci.org",
"node10.mpisws.vicci.org",
"node10.stanford.vicci.org",
"node11.stanford.vicci.org",
"node12.stanford.vicci.org",
"node13.stanford.vicci.org",
"node11.princeton.vicci.org",
"node13.princeton.vicci.org",
"node10.princeton.vicci.org",
"node1.stanford.vicci.org",
## new nodes
"node2.princeton.vicci.org",
"node2.stanford.vicci.org",
"node2.mpisws.vicci.org",
"node3.princeton.vicci.org",
"node3.stanford.vicci.org",
"node3.mpisws.vicci.org",
"node42.princeton.vicci.org",
"node4.stanford.vicci.org",
"node4.mpisws.vicci.org",
"node5.princeton.vicci.org",
"node5.stanford.vicci.org",
"node5.mpisws.vicci.org",
"node61.princeton.vicci.org",
"node6.stanford.vicci.org",
"node6.mpisws.vicci.org",
"node7.princeton.vicci.org",
"node7.stanford.vicci.org",
"node7.mpisws.vicci.org",
"node8.princeton.vicci.org",
"node8.stanford.vicci.org",
"node8.mpisws.vicci.org",
"node9.princeton.vicci.org",
"node9.stanford.vicci.org",
"node9.mpisws.vicci.org",

]
for server in servers:
  print server + " public_ip=" +socket.gethostbyname(server)
print "---------------"
for server in servers:
  print server,
