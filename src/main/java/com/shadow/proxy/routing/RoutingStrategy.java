package com.shadow.proxy.routing;

import com.shadow.proxy.protocol.DatabaseCommand;

public interface RoutingStrategy {
    BackendNode route(DatabaseCommand command);
}

