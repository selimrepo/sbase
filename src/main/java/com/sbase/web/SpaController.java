package com.sbase.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/** Serves the React application for browser routes while leaving API and actuator paths untouched. */
@Controller
public class SpaController {
    @GetMapping({"/", "/datasets", "/pipelines", "/mining", "/experiments", "/features", "/schedules", "/system"})
    public String index() {
        return "forward:/index.html";
    }
}
