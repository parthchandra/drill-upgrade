package org.apache.drill.upgrade;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;

/**
 * Created by pchandra on 11/13/15.
 */
public class UpgradePathFilter extends Utils.OutputFileUtils.OutputFilesFilter {
  public static final String HIDDEN_FILE_PREFIX = "_";
  public static final String DOT_FILE_PREFIX = ".";
  @Override
  public boolean accept(Path path) {
    if (path.getName().startsWith(HIDDEN_FILE_PREFIX)) {
      return false;
    }
    if (path.getName().startsWith(DOT_FILE_PREFIX)) {
      return false;
    }
    return super.accept(path);
  }

}
