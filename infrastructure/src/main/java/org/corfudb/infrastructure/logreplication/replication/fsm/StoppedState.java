package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * This class represents the stopped state of the Log Replication State Machine.
 *
 * This a termination state in the case of unrecoverable errors.
 **/
@Slf4j
public class StoppedState implements LogReplicationState {


    public StoppedState () {
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        return null;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        log.info("Unrecoverable error or explicit shutdown. " +
                "Log Replication is terminated from state {}. To resume, restart the JVM.", from.getType());
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.STOPPED;
    }
}
