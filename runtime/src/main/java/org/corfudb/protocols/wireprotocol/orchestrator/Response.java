package org.corfudb.protocols.wireprotocol.orchestrator;

/**
 * An interface that should be implemented by all the orchestrator service responses.
 * @author Maithem
 */
public interface Response {
    /**
     * Returns the type of the response.
     * @return type of response
     */
    OrchestratorResponseType getType();
}
