**Copied from https://github.com/dccmx/tcproxy.git. Used for connection issues simulation.**

tcproxy
=======
tcproxy is a small efficient tcp proxy that can be used for port forwarding or load balancing.

sample usage
------------
    tcproxy "11212 -> 11211"
    tcproxy "192.168.0.1:11212 -> 192.168.0.2:11211"

not implemented yet
---------------
    tcproxy "any:11212 -> rr{192.168.0.100:11211 192.168.0.101:11211 192.168.0.102:11211}"
    tcproxy "any:11212 -> hash{192.168.0.100:11211 192.168.0.101:11211 192.168.0.102:11211}"
