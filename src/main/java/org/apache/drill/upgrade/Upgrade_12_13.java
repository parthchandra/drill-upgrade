package org.apache.drill.upgrade;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

  private static String TMP_FILE_DIR = "/tmp";
  private static ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();

  private Configuration conf = new Configuration();
  private FileSystem fs;

  private void init() throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);
  }

  //get a list of files
  private void getFiles(String path, List<FileStatus> fileStatuses) throws IOException {
    Path p = Path.getPathWithoutSchemeAndAuthority(new Path(path));
    FileStatus fileStatus = fs.getFileStatus(p);
    if (fileStatus.isDirectory()) {
      for (FileStatus f : fs.listStatus(p, new UpgradePathFilter())) {
        getFiles(f.getPath().toString(), fileStatuses);
      }
    } else {
      if (fileStatus.isFile()) {
        fileStatuses.add(fileStatus);
      }
    }
  }

  public final Pair<ParquetMetadata, String> readFixedFooter(FileStatus file) throws IOException {
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
      return new Pair<ParquetMetadata, String>(parquetMetadata, origCreatedBy);
    } finally {
      f.close();
    }
  }

  private Pair<ParquetMetadata, String> getParquetFileMetadata(FileStatus file) throws IOException {
    Pair<ParquetMetadata,String> metadata = readFixedFooter(file);
    return metadata;
  }

  private void backupFile(FileStatus fileStatus) throws IOException {
    FileUtil.copy(fs, fileStatus.getPath(), fs, new Path(TMP_FILE_DIR), false, true, conf);
  }

  private boolean updateFile(FileStatus fileStatus, ParquetMetadata metadata /*, VersionParser.ParsedVersion version */)
      {
        try {
          //FileMetaData fm = metadata.getFileMetaData();
          //List<BlockMetaData> blocks = metadata.getBlocks();
          //String createdBy = Version.FULL_VERSION;
          //FileMetaData newFm = new FileMetaData(fm.getSchema(), fm.getKeyValueMetaData(), createdBy);
          //ParquetMetadata newParquetMetadata = new ParquetMetadata(newFm, blocks);

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
        }catch(Exception e){
          System.out.println("FAILURE : " + fileStatus.getPath().toString()+ ". Cause: "+ e.getMessage());
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
    FileUtil
        .copy(fs, new Path(TMP_FILE_DIR + "/" + fileStatus.getPath().getName()), fs, fileStatus.getPath(),
            false, true, conf);
  }

  private void removeBackupFile(FileStatus fileStatus) throws IOException {
    fs.delete(new Path(TMP_FILE_DIR + "/" + fileStatus.getPath().getName()));
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

  public static void main(String[] args) {
    if (args.length >= 1) {
      Upgrade_12_13 upgrade_12_13 = new Upgrade_12_13();
      try {
        upgrade_12_13.init();
      } catch (IOException e) {
        System.out.println("Initialization failed. (" + e.getMessage() + ")");
      }
      for (String path : args) {
        System.out.println("Executing Upgrade_12_13 on " + path);
        List<FileStatus> fileStatuses = new ArrayList<FileStatus>();
        try {
          upgrade_12_13.getFiles(path, fileStatuses);
        } catch (IOException e) {
          System.out.println("\tFailed to get list of file(s). Skipping. (" + e.getMessage() + ")");
        }
        for (FileStatus fileStatus : fileStatuses) {
          System.out.println("\tUpgrading " + fileStatus.getPath().toString());
          ParquetMetadata fixedMetadata;
          String origCreatedBy = "";
          VersionParser.ParsedVersion origVersion;
          try {
            Pair<ParquetMetadata, String> m = upgrade_12_13.getParquetFileMetadata(fileStatus);
            fixedMetadata=m.fst;
            origCreatedBy=m.snd;
            System.out.println("\t\t Created by :" + origCreatedBy);
            try {
              origVersion = VersionParser.parse(origCreatedBy);
            } catch (VersionParser.VersionParseException v) {
              upgrade_12_13.upgradeFile(fileStatus, fixedMetadata);
              continue;
            }
            //TODO: Check if no semver or if hasSemver then it is older than 1.8.0
            if (!origVersion.hasSemanticVersion() ) {
              upgrade_12_13.upgradeFile(fileStatus, fixedMetadata);
            } else {
              System.out.println("SKIPPED : " + fileStatus.getPath().toString());
            }
          } catch (Exception e) {
            System.out.println("\t\t\tFile skipped. (" + e.getMessage() + ")");
          }
        }
      }
      System.out.println("Done");
    } else {
      System.out.println("Nothing to do. I'm getting bored.");
    }
    return;
  }

}

