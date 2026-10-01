# LiftingCast client

A Go library that follows one LiftingCast meet live and hands the current meet state to in-process consumers.

## Language

**Meet state**:
The full picture of one meet, made up of its sections and built up from the messages LiftingCast sends. Each message carries one or more sections.
_Avoid_: meet data, snapshot

**Section**:
One top-level part of the meet state: the meet name, units or federation, lifters, platforms, divisions or teams. LiftingCast sends each section complete or not at all, so the newest copy of a section replaces the previous one, and anything missing from it no longer exists upstream.
_Avoid_: key, part

**Upstream connection**:
The single live link to LiftingCast for one meet and one set of credentials. It keeps trying to connect until it is deliberately closed. Switching meets or credentials means replacing it, not changing it.
_Avoid_: socket, session

**Connection status**:
Whether the upstream connection is connected right now, and the most recent error it saw. Connected means LiftingCast accepted the connection, not that the credentials are known to be good. It describes the connection itself, not how far consumers have got through the meet states already sent to them.
_Avoid_: health, online

**Server error**:
A message LiftingCast sends on a healthy upstream connection that is not meet state. It sets the last error but leaves the connection connected.

**Listener**:
An in-process consumer that receives every meet state in order. It outlives any one upstream connection and stops only when unregistered or replaced by another listener with the same ID.
_Avoid_: subscriber, client
