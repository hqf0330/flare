package com.bhcode.flare.flink.starter;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TemplateRenderer {

    public String renderClasspathTemplate(String resourcePath, Map<String, String> placeholders) {
        if (resourcePath == null || resourcePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Template path cannot be empty");
        }
        InputStream input = TemplateRenderer.class.getResourceAsStream(resourcePath);
        if (input == null) {
            throw new IllegalArgumentException("Template not found: " + resourcePath);
        }
        try (InputStream in = input) {
            String template = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return render(template, placeholders);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load template: " + resourcePath, e);
        }
    }

    public String render(String template, Map<String, String> placeholders) {
        if (template == null) {
            return "";
        }
        String rendered = template;
        if (placeholders == null || placeholders.isEmpty()) {
            return rendered;
        }
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isEmpty()) {
                continue;
            }
            String value = entry.getValue() == null ? "" : entry.getValue();
            rendered = rendered.replace("${" + key + "}", value);
        }
        return rendered;
    }
}
