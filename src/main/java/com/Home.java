package com;

import com.mapreduce.KnnMapReduce;

import java.io.IOException;

public class Home {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        System.out.println("Hadoop execution time = " + KnnMapReduce.execute()); // blocking -> wait till job is complete for application to exit/show user output
    }
}
