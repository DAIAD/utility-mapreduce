package eu.daiad.mapreduce.hbase.job;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import eu.daiad.mapreduce.hbase.EnumHadoopParameter;
import eu.daiad.mapreduce.hbase.EnumJobMapReduceParameter;

/**
 * Helper class for implementing the {@link Tool} interface for executing jobs
 * that are using HBase tables as a source.
 */
public abstract class HBaseMapReduceJob extends Configured implements Tool {

    /**
     * Configures the job.
     *
     * @param conf the job configuration.
     * @param args command specific arguments passed when invoking {@link Tool#run(String[])}.
     * @return the configured job.
     * @throws IOException if HBase table scanner initialization fails.
     */
    protected Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        Job job = Job.getInstance(conf, conf.get(EnumJobMapReduceParameter.JOB_NAME.getValue()));

        job.setJarByClass(getJobClass());

        addArchiveToClassPath(conf, job);

        addFilesToCache(conf, job);

        configureJob(job);

        setScans(conf, job);

        return job;
    }

    /**
     * Returns the {@link Class} that implements the job.
     *
     * @return the {@link Class} implementing the job.
     */
	protected abstract Class<?> getJobClass();

	/**
	 * Initializes the HBase table scanners for the job.
	 *
	 * @param conf the job configuration.
	 * @param job the job being initialized.
	 * @throws IOException if scanner initialization fails.
	 */
	protected abstract void setScans(Configuration conf, Job job) throws IOException;

	/**
	 * Performs any job specific initialization.
	 *
	 * @param job the job being initialized.
	 */
	protected abstract void configureJob(Job job);

    /**
     * Add one or more archive paths to the current set of classpath entries. It
     * adds the archives to cache as well.
     *
     * Archive files will be unpacked and added to the classpath when being
     * distributed.
     *
     * @param conf the job configuration.
     * @param job the job being configured.
     * @throws IOException
     */
	private void addArchiveToClassPath(Configuration conf, Job job) throws IOException {
        String localJarsDir = conf.get(EnumJobMapReduceParameter.LOCAL_LIB_PATH.getValue());
        String hdfsJarsDir = conf.get(EnumJobMapReduceParameter.HDFS_LIB_PATH.getValue());

        if (!StringUtils.isBlank(localJarsDir)) {
            copyLocalJarsToHdfs(localJarsDir, hdfsJarsDir, job.getConfiguration());

            Set<Path> jarPaths = collectJarPathsOnHdfs(hdfsJarsDir, job.getConfiguration());

            if (!jarPaths.isEmpty()) {
                // https://issues.apache.org/jira/browse/HADOOP-9123
                System.setProperty("path.separator", ":");

                for (Path jarPath : jarPaths) {
                    job.addArchiveToClassPath(jarPath);
                }
            }
        }
	}

    /**
     * Add one or more files to distributed cache.
     *
     * @param conf the job configuration.
     * @param job the job being configured.
     * @throws IOException
     */
	private void addFilesToCache(Configuration conf, Job job) throws IOException {
        checkRequiredArgument(conf, "Configuration is null");

	    String hdfsPathDir = conf.get(EnumJobMapReduceParameter.HDFS_CACHE_PATH.getValue());

        if (!StringUtils.isBlank(hdfsPathDir)) {
            FileSystem hdfsFileSystem = FileSystem.get(conf);

            Path hdfsPath = new Path(hdfsPathDir);

            if(hdfsFileSystem.exists(hdfsPath)) {
                FileStatus[] fileStatuses = hdfsFileSystem.listStatus(hdfsPath);
                for (FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.isDirectory()) {
                        job.addCacheFile(fileStatus.getPath().toUri());
                    }
                }
            }
        }
    }

	/**
	 * Copies jar files from the local path to the remote HDFS path.
	 *
	 * @param localJarsDir the local path.
	 * @param hdfsJarsDir the remote path.
	 * @param conf the job configuration.
	 * @throws IOException if an I/O error occurs.
	 */
	private void copyLocalJarsToHdfs(String localJarsDir, String hdfsJarsDir, Configuration conf) throws IOException {
        checkRequiredArgument(hdfsJarsDir, "HDFS JARs dir is null");
        checkRequiredArgument(conf, "Configuration is null");

        // Find all jar files inside the local path.
        Set<File> jarFiles = collectJarFilesFromLocalDir(localJarsDir);

        if (!jarFiles.isEmpty()) {
            System.err.println(String.format("Copying [%d] JAR files from local dir [%s] to HDFS dir [%s] at [%s]",
                                             jarFiles.size(),
                                             localJarsDir,
                                             hdfsJarsDir,
                                             resolveHdfsAddress(conf)));

            FileSystem hdfsFileSystem = FileSystem.get(conf);

            for (File jarFile : jarFiles) {
                Path localJarPath = new Path(jarFile.toURI());
                Path hdfsJarPath = new Path(hdfsJarsDir, jarFile.getName());
                hdfsFileSystem.copyFromLocalFile(false, true, localJarPath, hdfsJarPath);
            }
        }
	}

    /**
     * Checks if the given argument is null.
     *
     * @param argument the argument.
     * @param errorMessage the error message of the exception if the argument is null.
     * @throws IllegalArgumentException if argument is null.
     */
	private void checkRequiredArgument(Object argument, String errorMessage) throws IllegalArgumentException {
        if (argument == null) {
            throw new IllegalArgumentException(errorMessage);
        }
	}

	/**
     * Returns a set of all jar files in the given path.
     *
     * @param localJarsDirPath the local path.
     * @return a set of jar files.
     * @throws IllegalArgumentException if {@code localJarsDirPath} does not exist.
     */
	private Set<File> collectJarFilesFromLocalDir(String localJarsDirPath) throws IllegalArgumentException {
        File path = new File(localJarsDirPath);

        if (!path.isDirectory()) {
            throw new IllegalArgumentException(String.format("Path points to file, not directory: %s", localJarsDirPath));
        }

        Set<File> jarFiles = new HashSet<File>();
        for (File libFile : path.listFiles()) {
            if (libFile.exists() && !libFile.isDirectory() && libFile.getName().endsWith(".jar")) {
                jarFiles.add(libFile);
            }
        }
        return jarFiles;
	}

	/**
	 * Returns the HDFS path.
	 *
	 * @param configuration the job configuration.
	 * @return the HDFS path.
	 */
	private String resolveHdfsAddress(Configuration configuration) {
		return configuration.get(EnumHadoopParameter.HDFS_PATH.getValue());
	}

	/**
	 * Returns a set of jar files stored at a remote HDFS path.
	 *
	 * @param hdfsJarsDir the HDFS path to search.
	 * @param configuration the job configuration.
	 * @return a set of jar files.
	 * @throws IOException if an I/O exception occurs.
	 */
	private Set<Path> collectJarPathsOnHdfs(String hdfsJarsDir, Configuration configuration) throws IOException {
        Set<Path> jarPaths = new HashSet<Path>();

        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsPath = new Path(hdfsJarsDir);

        if (!fileSystem.exists(hdfsPath)) {
            throw new IllegalArgumentException("Directory ['" + hdfsJarsDir + "'] doesn't exist on HDFS [" + resolveHdfsAddress(configuration) + "].");
        }
        if (fileSystem.isFile(hdfsPath)) {
            throw new IllegalArgumentException("Path ['" + hdfsJarsDir + "'] on HDFS [" + resolveHdfsAddress(configuration) + "] is a file, not directory.");
        }

        FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDirectory()) {
                jarPaths.add(fileStatus.getPath());
            }
        }
        return jarPaths;
	}

    /**
     * Helper method for computing the closest next row key to the given row key
     * prefix.
     *
     * @param rowKeyPrefix the row key prefix.
     * @return the closest row key next to {@code rowKeyPrefix}.
     */
    protected byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
        // Essentially we are treating it like an 'unsigned very very long' and
        // doing +1 manually. Search for the place where the trailing 0xFFs
        // start
        int offset = rowKeyPrefix.length;
        while (offset > 0) {
            if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                break;
            }
            offset--;
        }

        if (offset == 0) {
            // We got an 0xFFFF... (only FFs) stopRow value which is
            // the last possible prefix before the end of the table.
            // So set it to stop at the 'end of the table'
            return HConstants.EMPTY_END_ROW;
        }

        // Copy the right length of the original
        byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
        // And increment the last one
        newStopRow[newStopRow.length - 1]++;
        return newStopRow;
    }
}
