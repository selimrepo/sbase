package io.sbase.server.catalog;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CatalogServiceTest {
    @Test void rejectsMutatingSqlBeforeOpeningConnection() {
        CatalogService service=new CatalogService();
        assertThrows(IllegalArgumentException.class,()->service.queryPostgres("DROP TABLE users",List.of(),100));
        assertThrows(IllegalArgumentException.class,()->service.queryPostgres("SELECT 1; DROP TABLE users",List.of(),100));
    }

    @Test void rejectsMutatingCqlBeforeOpeningConnection() {
        CatalogService service=new CatalogService();
        assertThrows(IllegalArgumentException.class,()->service.queryCassandra("DELETE FROM users",100));
    }
}
