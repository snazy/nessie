/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.quarkus.cli;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.Consumer;
import org.projectnessie.versioned.transfer.ExportImportConstants;
import org.projectnessie.versioned.transfer.NessieExporter;
import org.projectnessie.versioned.transfer.ProgressEvent;
import org.projectnessie.versioned.transfer.ProgressListener;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.ZipArchiveExporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import picocli.CommandLine;
import picocli.CommandLine.PicocliException;

@CommandLine.Command(
    name = "export",
    mixinStandardHelpOptions = true,
    description = "Exports a Nessie repository to the local file system.")
public class ExportRepository extends BaseCommand {

  static final String PATH = "--path";
  static final String OUTPUT_FORMAT = "--output-format";
  static final String MAX_FILE_SIZE = "--max-file-size";
  static final String EXPECTED_COMMIT_COUNT = "--expected-commit-count";
  static final String OUTPUT_BUFFER_SIZE = "--output-buffer-size";

  enum Format {
    ZIP,
    DIRECTORY
  }

  @CommandLine.Option(
      names = {"-p", PATH},
      required = true,
      paramLabel = "<export-to>",
      description = "The ZIP file or directory to create with the export contents.")
  private Path path;

  @CommandLine.Option(
      names = {"-F", OUTPUT_FORMAT},
      paramLabel = "<output-format>",
      description = {
        "Explicitly define the output format to use to the export.",
        "If not specified, the implementation chooses the ZIP export, if "
            + PATH
            + " ends in .zip, otherwise will use the directory output format.",
        "Possible values: ${COMPLETION-CANDIDATES}"
      })
  private Format outputFormat;

  @CommandLine.Option(
      names = MAX_FILE_SIZE,
      description = "Maximum size of a file in bytes inside the export.")
  private Long maxFileSize;

  @CommandLine.Option(
      names = {"-C", EXPECTED_COMMIT_COUNT},
      description =
          "Expected number of commits in the repository, defaults to "
              + ExportImportConstants.DEFAULT_EXPECTED_COMMIT_COUNT
              + ".")
  private Integer expectedCommitCount;

  @CommandLine.Option(
      names = OUTPUT_BUFFER_SIZE,
      description =
          "Output buffer size, defaults to " + ExportImportConstants.DEFAULT_BUFFER_SIZE + ".")
  private Integer outputBufferSize;

  @CommandLine.Option(
      names = {"--full-scan"},
      description = {
        "Export all commits, including those that are no longer reachable any named reference."
            + "Using this option is _not_ recommended."
      })
  private boolean fullScan;

  @Override
  protected Integer callWithDatabaseAdapter() throws Exception {
    return export(b -> b.databaseAdapter(databaseAdapter));
  }

  @Override
  protected Integer callWithPersist() throws Exception {
    return export(b -> b.persist(persist));
  }

  Integer export(Consumer<NessieExporter.Builder> builderConsumer) throws Exception {
    warnOnInMemory();

    spec.commandLine()
        .getOut()
        .printf("Exporting from a %s version store...%n", versionStoreConfig.getVersionStoreType());

    try (ExportFileSupplier exportFileSupplier = createExportFileSupplier()) {
      NessieExporter.Builder builder =
          NessieExporter.builder().exportFileSupplier(exportFileSupplier).fullScan(fullScan);
      builderConsumer.accept(builder);
      if (maxFileSize != null) {
        builder.maxFileSize(maxFileSize);
      }
      if (expectedCommitCount != null) {
        builder.expectedCommitCount(expectedCommitCount);
      }
      if (outputBufferSize != null) {
        builder.outputBufferSize(outputBufferSize);
      }

      PrintWriter out = spec.commandLine().getOut();

      builder.progressListener(new ExportProgressListener(out)).build().exportNessieRepository();

      return 0;
    }
  }

  private ExportFileSupplier createExportFileSupplier() {
    ExportFileSupplier exportFileSupplier;
    switch (exportFormat()) {
      case ZIP:
        if (Files.isRegularFile(path)) {
          throw new PicocliException(
              String.format(
                  "Export file %s already exists, please delete it first, if you want to overwrite it.",
                  path));
        }
        exportFileSupplier = ZipArchiveExporter.builder().outputFile(path).build();
        break;
      case DIRECTORY:
        if (Files.isRegularFile(path)) {
          throw new PicocliException(
              String.format("%s refers to a file, but export type is %s.", path, Format.DIRECTORY));
        }
        exportFileSupplier = FileExporter.builder().targetDirectory(path).build();
        break;
      default:
        throw new IllegalStateException(exportFormat().toString());
    }
    return exportFileSupplier;
  }

  private Format exportFormat() {
    if (outputFormat != null) {
      return outputFormat;
    }

    String fileName = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return fileName.endsWith(".zip") ? Format.ZIP : Format.DIRECTORY;
  }

  /** Mostly paints dots - but also some numbers about the progress. */
  private static final class ExportProgressListener implements ProgressListener {
    private final PrintWriter out;
    private int count;
    private boolean dot;
    private ExportMeta exportMeta;

    private ExportProgressListener(PrintWriter out) {
      this.out = out;
    }

    @Override
    public void progress(@Nonnull ProgressEvent progress, ExportMeta meta) {
      switch (progress) {
        case FINISHED:
          out.printf(
              "Exported Nessie repository, %d commits into %d files, %d named references into %d files.%n",
              exportMeta.getCommitCount(),
              exportMeta.getCommitsFilesCount(),
              exportMeta.getNamedReferencesCount(),
              exportMeta.getNamedReferencesFilesCount());
          break;
        case END_META:
          this.exportMeta = meta;
          break;
        case START_COMMITS:
          out.println("Exporting commits...");
          count = 0;
          dot = false;
          break;
        case END_COMMITS:
          if (dot) {
            out.println();
          }
          out.printf("%d commits exported.%n%n", count);
          break;
        case START_NAMED_REFERENCES:
          out.println("Exporting named references...");
          count = 0;
          dot = false;
          break;
        case COMMIT_WRITTEN:
        case NAMED_REFERENCE_WRITTEN:
          count++;
          if ((count % 10) == 0) {
            out.print('.');
            dot = true;
          }
          if ((count % 500) == 0) {
            out.printf(" %d%n", count);
            dot = false;
          }
          break;
        case END_NAMED_REFERENCES:
          if (dot) {
            out.println();
          }
          out.printf("%d named references exported.%n%n", count);
          break;
        case START_FINALIZE:
          out.printf("Finalizing export...%n");
          dot = false;
          break;
        case END_FINALIZE:
          if (dot) {
            out.println();
          }
          out.printf("Export finalization finished.%n%n");
          break;
        default:
          break;
      }
    }
  }
}
