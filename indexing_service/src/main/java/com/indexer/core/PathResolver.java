package com.indexer.core;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class PathResolver {

    private final Path lakeRoot;

    public PathResolver(Path lakeRoot) {
        this.lakeRoot = lakeRoot;
    }

    public Path resolve(String lakePath) {
        Path p = Paths.get(lakePath);

        if (p.isAbsolute()) return p;

        String normalized = lakePath.replace("\\", "/");
        String rootNorm = lakeRoot.toString().replace("\\", "/");

        if (normalized.startsWith(rootNorm)) {
            return Paths.get(lakePath);
        }

        return lakeRoot.resolve(lakePath);
    }
}