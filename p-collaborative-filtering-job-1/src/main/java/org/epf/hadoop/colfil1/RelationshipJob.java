package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelationshipJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RelationshipJob <input path> <output path>");
            System.exit(-1);
        }

        // Configuration du job Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Relationship Job");

        // Spécification des classes Map et Reduce
        job.setJarByClass(RelationshipJob.class);
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);

        // Définition des types de clé et de valeur pour le job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Spécification des formats d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
