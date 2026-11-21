package io.sbase.server.lakehouse;

import java.util.UUID;

/** Request/job scoped identity propagated to synchronous Spark query listeners. */
public final class LakehouseExecutionContext implements AutoCloseable {
    private record Identity(UUID workspaceId,String principal) {}
    private static final ThreadLocal<Identity> CURRENT=new ThreadLocal<>();
    private final Identity previous;

    private LakehouseExecutionContext(UUID workspaceId,String principal) {
        previous=CURRENT.get();CURRENT.set(new Identity(workspaceId,principal));
    }
    public static LakehouseExecutionContext enter(UUID workspaceId,String principal) {
        if(workspaceId==null) throw new IllegalArgumentException("workspaceId is required");
        return new LakehouseExecutionContext(workspaceId,principal==null?"system":principal);
    }
    public static UUID workspaceId() { Identity v=CURRENT.get();return v==null?null:v.workspaceId(); }
    public static String principal() { Identity v=CURRENT.get();return v==null?"spark":v.principal(); }
    @Override public void close() { if(previous==null) CURRENT.remove();else CURRENT.set(previous); }
}
