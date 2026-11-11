package com.sbase.platform;

import com.sbase.platform.security.PermissionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/platform/advanced/dashboards")
public class DashboardLayoutController {
    private final JdbcTemplate db;
    private final PermissionService permissions;

    public DashboardLayoutController(@Qualifier("jdbcTemplate") JdbcTemplate db, PermissionService permissions) {
        this.db = db;
        this.permissions = permissions;
    }

    public record WidgetPosition(UUID id, int x, int y, int width, int height) {}
    public record LayoutRequest(@NotEmpty List<@Valid WidgetPosition> widgets) {}

    @PutMapping("/{id}/layout")
    @Transactional
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void saveLayout(@PathVariable UUID id, @Valid @RequestBody LayoutRequest request, Authentication auth) {
        permissions.require(auth, "bi:write");
        if (db.queryForObject("SELECT count(*) FROM dashboards WHERE id=? AND owner_subject=?", Integer.class, id, auth.getName()) == 0)
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Dashboard not found");
        if (request.widgets().size() > 100) throw new IllegalArgumentException("A dashboard can contain at most 100 widgets");
        Set<UUID> ids = new HashSet<>();
        for (WidgetPosition widget : request.widgets()) {
            if (!ids.add(widget.id())) throw new IllegalArgumentException("Duplicate widget id in layout");
            if (widget.x() < 0 || widget.x() > 11 || widget.y() < 0 || widget.y() > 10000
                    || widget.width() < 1 || widget.width() > 12 || widget.x() + widget.width() > 12
                    || widget.height() < 1 || widget.height() > 100)
                throw new IllegalArgumentException("Widget position is outside the 12-column dashboard grid");
        }
        for (int i = 0; i < request.widgets().size(); i++) {
            WidgetPosition a = request.widgets().get(i);
            for (int j = i + 1; j < request.widgets().size(); j++) {
                WidgetPosition b = request.widgets().get(j);
                boolean overlap = a.x() < b.x() + b.width() && a.x() + a.width() > b.x()
                    && a.y() < b.y() + b.height() && a.y() + a.height() > b.y();
                if (overlap) throw new IllegalArgumentException("Dashboard widgets cannot overlap");
            }
        }
        for (WidgetPosition widget : request.widgets()) {
            int changed = db.update("""
                UPDATE dashboard_widgets
                   SET position_x=?, position_y=?, width=?, height=?
                 WHERE id=? AND dashboard_id=?
                """, widget.x(), widget.y(), widget.width(), widget.height(), widget.id(), id);
            if (changed != 1) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Widget does not belong to this dashboard");
        }
        db.update("UPDATE dashboards SET version=version+1, updated_at=now() WHERE id=?", id);
    }
}
