package com;

public class Globals {

    /**
     * Returns the URL of the Hadoop Namenode
     *
     * @return String depicting URL of the Hadoop Namenode
     */
    public static String getNamenodeUrl() {
        return "hdfs://localhost:9000";
    }

    /**
     * Returns the URL for Hadoop WebHDFS
     *
     * @return String depicting URL of Hadoop WebHDFS
     */
    public static String getWebhdfsHost() {
        return "http://localhost:9870";
    }

    /**
     * Returns the relative HDFS input path of the CSV files being used as tables.
     *
     * @return String depicting relative input path of the CSV files being used as tables.
     */
    public static String getFileInputPath() {
        return "/input/";
    }

    /**
     * Returns the relative HDFS output path of the Hadoop Job
     *
     * @return String depicting relative HDFS output path of the Hadoop Job
     */
    public static String getHadoopOutputPath() {
        return "/output/hadoop/";
    }

    /**
     * Hardcoded value of 5 for temporary use
     *
     * @return The number of nearest neighbors that KNN should use for classification
     */
    public static int getKvalue() {
        return 5;
    }

    /**
     * Return the name of the test file
     *
     * @return String representing the name of the file containing test data
     */
    public static String getTestFileName() {
        return "shuttle.tst";
    }

    /**
     * Return the name of the train file
     *
     * @return String representing the name of the file containing training data
     */
    public static String getTrainFileName() {
        return "shuttle.trn";
    }
}
