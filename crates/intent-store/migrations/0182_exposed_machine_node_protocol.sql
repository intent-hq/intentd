-- The node-link protocol version the backend's last bind with an exposed
-- machine negotiated, served as `RemoteMachine.nodeProtocol` and kept while
-- the machine is disconnected. NULL before the first bind. Remote agents'
-- saved scripts need nodeProtocol 8.
ALTER TABLE exposed_machine ADD COLUMN node_protocol INTEGER;
