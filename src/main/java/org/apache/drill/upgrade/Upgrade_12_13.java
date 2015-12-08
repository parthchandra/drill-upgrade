package org.apache.drill.upgrade;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.format.Util.writeFileMetaData;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

public class Upgrade_12_13 {

  private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Upgrade_12_13.class);

  private Configuration conf = new Configuration();
  private FileSystem fs;
  private String tmpFileDir = "/tmp";


  private static class MetadataUpgradeInfo {
    public ParquetMetadata metadata;
    public String createdBy;

    public MetadataUpgradeInfo(ParquetMetadata metadata, String createdBy) {
      this.metadata = metadata;
      this.createdBy = createdBy;
    }
  }

  private void init() throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);
  }

  //get a list of files to process
  private void getFiles(String path, List<FileStatus> fileStatuses, List<FileStatus> dirs) throws IOException {
    Path p = Path.getPathWithoutSchemeAndAuthority(new Path(path));
    FileStatus fileStatus = fs.getFileStatus(p);
    if (fileStatus.isDirectory()) {
      dirs.add(fileStatus);
      for (FileStatus f : fs.listStatus(p, new UpgradePathFilter())) {
        getFiles(f.getPath().toString(), fileStatuses, dirs);
      }
    } else {
      if (fileStatus.isFile()) {
        fileStatuses.add(fileStatus);
      }
    }
  }

  private final MetadataUpgradeInfo readFixedFooter(FileStatus file) throws IOException {
    FSDataInputStream f = fs.open(file.getPath());
    try {
      long l = file.getLen();
      int FOOTER_LENGTH_SIZE = 4;
      if (l < MAGIC.length + FOOTER_LENGTH_SIZE
          + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
        throw new RuntimeException(file.getPath() + " is not a Parquet file (too small)");
      }
      long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;

      f.seek(footerLengthIndex);
      int footerLength = readIntLittleEndian(f);
      byte[] magic = new byte[MAGIC.length];
      f.readFully(magic);
      if (!Arrays.equals(MAGIC, magic)) {
        throw new RuntimeException(
            file.getPath() + " is not a Parquet file. expected magic number at tail " + Arrays
                .toString(MAGIC) + " but found " + Arrays.toString(magic));
      }
      long footerIndex = footerLengthIndex - footerLength;
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException("corrupted file: the footer index is not within the file");
      }
      f.seek(footerIndex);

      // This is parquet.format.FileMetaData not parquet.hadoop.metadata.FileMetaData;
      org.apache.parquet.format.FileMetaData fileFormatMetaData = Util.readFileMetaData(f);
      String origCreatedBy = fileFormatMetaData.getCreated_by();

      // Set the created_by String to a valid string so that the metadata converter call can
      // correctly return the Parquet metadata. This also sets the created by string to what we want
      // to write to the footer
      fileFormatMetaData.setCreated_by(Version.FULL_VERSION);

      ParquetMetadata parquetMetadata = metadataConverter.fromParquetMetadata(fileFormatMetaData);
      return new MetadataUpgradeInfo(parquetMetadata, origCreatedBy);
    } finally {
      f.close();
    }
  }

  private MetadataUpgradeInfo getParquetFileMetadata(FileStatus file) throws IOException {
    MetadataUpgradeInfo metadata = readFixedFooter(file);
    return metadata;
  }

  private void backupFile(FileStatus fileStatus) throws IOException {
    FileUtil.copy(fs, fileStatus.getPath(), fs, new Path(tmpFileDir), false, true, conf);
  }

  private boolean updateFile(FileStatus fileStatus, ParquetMetadata metadata ) {
    try {
      FSDataOutputStream to;
      //TODO: Check files created using ChecksumFileSystem after the upgrade
      // Checksum file system does not support 'append'. We write the file without a checksum (Does this
      // cause reads to fail ??
      if (fs instanceof ChecksumFileSystem) {
        to = ((ChecksumFileSystem) fs).getRawFileSystem().append(fileStatus.getPath());
      } else {
        to = fs.append(fileStatus.getPath());
      }
      serializeFooter(metadata, to);
      to.close();
    } catch (Exception e) {
      System.out.println("FAILURE : " + fileStatus.getPath().toString() + ". Cause: " + e.getMessage());
      return false;
    }
    System.out.println("SUCCESS : " + fileStatus.getPath().toString());
    return true;
  }

  private void serializeFooter(ParquetMetadata footer, FSDataOutputStream out) throws IOException {
    long footerIndex = out.getPos();
    org.apache.parquet.format.FileMetaData parquetMetadata =
        metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    writeFileMetaData(parquetMetadata, out);
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(ParquetFileWriter.MAGIC);
  }

  private void restoreBackupFile(FileStatus fileStatus) throws IOException {
    FileUtil.copy(fs, new Path(tmpFileDir + "/" + fileStatus.getPath().getName()), fs, fileStatus.getPath(),
        false, true, conf);
  }

  private void removeBackupFile(FileStatus fileStatus) throws IOException {
    fs.delete(new Path(tmpFileDir + "/" + fileStatus.getPath().getName()));
  }

  private void touchDirs(List<FileStatus> dirs){
    long ts = System.currentTimeMillis();
    for(FileStatus d : dirs){
      try {
        fs.setTimes(d.getPath(), ts, ts);
      } catch (IOException e) {
        logger.debug("Unable to update directory timestamp [ {} ].", d.toString());
      }
    }
  }

  private boolean upgradeFile(FileStatus fileStatus, ParquetMetadata metadata) throws IOException {
    backupFile(fileStatus);
    boolean ret = updateFile(fileStatus, metadata);
    if (ret) {
      removeBackupFile(fileStatus);
    } else {
      restoreBackupFile(fileStatus);
    }
    return ret;
  }


  private static final SemanticVersion PARQUET_251_FIXED_VERSION = new SemanticVersion(1, 8, 0);
  private static final SemanticVersion CDH_5_PARQUET_251_FIXED_START =
      new SemanticVersion(1, 5, 0, (String) null, "cdh5.5.0", (String) null);
  private static final SemanticVersion CDH_5_PARQUET_251_FIXED_END = new SemanticVersion(1, 5, 0);

  boolean needsUpgrade(SemanticVersion semver) {
    if (semver.compareTo(PARQUET_251_FIXED_VERSION) < 0 && (
        semver.compareTo(CDH_5_PARQUET_251_FIXED_START) < 0
            || semver.compareTo(CDH_5_PARQUET_251_FIXED_END) >= 0)) {
      return true;
    }
    return false;
  }

  private void printUsage(){
    System.out.println(
        "Usage: ");
    System.out.println(
        "\tjava -Dlog.path=/path/to/logfile.log -cp drill-upgrade-1.0-jar-with-dependencies.jar org.apache.drill.upgrade.Upgrade_12_13 --tempDir=/path/to/tempdir list_of_dirs_or_files ");
    System.out.println(
        "\tjava -Dlog.path=/path/to/logfile.log -cp drill-upgrade-1.0-jar-with-dependencies.jar org.apache.drill.upgrade.Upgrade_12_13  list_of_dirs_or_files ");
    System.out.println(
        "\tjava -cp drill-upgrade-1.0-jar-with-dependencies.jar org.apache.drill.upgrade.Upgrade_12_13 --help ");
    System.out.println("");
    return;
  }

  public static void main(String[] args) {
    List<String> dirs = new ArrayList<String>();
    if (args.length >= 1) {
      Upgrade_12_13 upgrade_12_13 = new Upgrade_12_13();
      try {
        upgrade_12_13.init();
      } catch (IOException e) {
        logger.info("Initialization failed. (" + e.getMessage() + ")");
      }
      for (String arg : args) {
        if (arg.startsWith("--help")) {
          upgrade_12_13.printUsage();
          return;
        } else if (arg.startsWith("--tempDir")) {
          String a[] = arg.split("=");
          if(a.length!=2){
            System.out.println("The option --tempDir expects a directory name.");
            upgrade_12_13.printUsage();
            return;
          }
          upgrade_12_13.tmpFileDir = a[1];
        } else {
          dirs.add(arg);
        }
      }
      for (String path : dirs) {
        logger.info("Executing Upgrade_12_13 on " + path);
        List<FileStatus> fileStatuses = new ArrayList<FileStatus>();
        List<FileStatus> dirStatuses = new ArrayList<FileStatus>();
        try {
          upgrade_12_13.getFiles(path, fileStatuses, dirStatuses);
        } catch (IOException e) {
          logger.error("Failed to get list of file(s). Skipping. (" + e.getMessage() + ")");
        }
        for (FileStatus fileStatus : fileStatuses) {
          logger.info("Upgrading " + fileStatus.getPath().toString());
          ParquetMetadata fixedMetadata;
          String origCreatedBy = "";
          VersionParser.ParsedVersion origVersion;
          try {
            MetadataUpgradeInfo m = upgrade_12_13.getParquetFileMetadata(fileStatus);
            fixedMetadata = m.metadata;
            origCreatedBy = m.createdBy;
            logger.debug("Created by :" + origCreatedBy);
            try {
              origVersion = VersionParser.parse(origCreatedBy);
            } catch (VersionParser.VersionParseException v) {
              upgrade_12_13.upgradeFile(fileStatus, fixedMetadata);
              continue;
            }
            //TODO: Check if no semver or if hasSemver then it is older than 1.8.0
            if (!origVersion.hasSemanticVersion() || upgrade_12_13
                .needsUpgrade(origVersion.getSemanticVersion())) {
              upgrade_12_13.upgradeFile(fileStatus, fixedMetadata);
            } else {
              System.out.println(
                  "SKIPPED : " + fileStatus.getPath().toString() + ". File version did not need upgrade. ["
                      + origCreatedBy + "]");
            }
          } catch (Exception e) {
            System.out
                .println("FAILURE : " + fileStatus.getPath().toString() + ". Cause: " + e.getMessage());
          }
        }
        logger.debug("Updating directory timestamps");
        upgrade_12_13.touchDirs(dirStatuses);
      }
      System.out.println("Done");
    } else {
      System.out.println("Nothing to do. I'm getting bored.");
    }
    return;
  }
}

