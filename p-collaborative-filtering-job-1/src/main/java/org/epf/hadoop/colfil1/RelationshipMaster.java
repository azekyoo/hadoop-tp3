package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Lire la ligne et la convertir en Relationship
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length == 3) {
            // Extraire les informations de la relation
            String user1 = fields[0].trim();
            String user2 = fields[1].trim();
            String relationshipType = fields[2].trim();

            // Créer deux paires (user1, user2) et (user2, user1)
            // La clé est l'utilisateur, la valeur est l'utilisateur lié.
            context.write(new Text(user1), new Text(user2));
            context.write(new Text(user2), new Text(user1));
        }
    }
}
