package com.spd.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DpiPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        return !path.getName().contains(".tmp");
    }
}
