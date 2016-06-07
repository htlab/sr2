package soxrecorderv2.common.model;

public class NodeIdentifier {
	
	private final String server;
	private final String node;
	
	public NodeIdentifier(String server, String node) {
		if (server == null) {
			throw new IllegalArgumentException("server cannot be null");
		}
		if (node == null) {
			throw new IllegalArgumentException("node cannot be null");
		}
		this.server = server;
		this.node = node;
	}
	
	public String getServer() {
		return server;
	}
	
	public String getNode() {
		return node;
	}
	
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeIdentifier)) {
			return false;
		}
		
		NodeIdentifier otherNodeID = (NodeIdentifier)other;
		String oServer = otherNodeID.getServer();
		String oNode = otherNodeID.getNode();
		return (oServer.equals(server) && oNode.equals(node));
	}
	
	@Override
	public int hashCode() {
		final int mask = 0xABCD0123;
		return (server.hashCode() ^ node.hashCode() ^ mask);
	}

}
