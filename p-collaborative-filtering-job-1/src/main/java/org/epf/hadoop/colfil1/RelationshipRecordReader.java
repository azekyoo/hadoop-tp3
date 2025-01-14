package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {

    // LineRecordReader lit les lignes du fichier
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable(); // Clé : numéro de ligne
    private Relationship currentValue = new Relationship(); // Valeur : un objet Relationship

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // Initialisation du LineRecordReader avec le split et le contexte
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNext = lineRecordReader.nextKeyValue(); // Vérifie s'il reste des lignes

        if (hasNext) {
            // Si la ligne suivante existe, nous mettons à jour la clé (numéro de ligne)
            currentKey.set(lineRecordReader.getCurrentKey().get()); // La clé correspond à l'index de la ligne

            // Récupérer la ligne actuelle depuis le LineRecordReader
            String line = lineRecordReader.getCurrentValue().toString(); // La ligne sous forme de texte

            // Exemple de parsing de la ligne (assumons que les champs sont séparés par des virgules)
            String[] fields = line.split(",");

            if (fields.length == 3) {
                // Si la ligne a 3 parties (par exemple : user1, user2, type de relation)
                String user1 = fields[0].trim(); // Premier utilisateur
                String user2 = fields[1].trim(); // Deuxième utilisateur
                String relationshipType = fields[2].trim(); // Type de relation

                // Créer un objet Relationship avec les données extraites
                currentValue.setUser1(user1);
                currentValue.setUser2(user2);
                currentValue.setRelationshipType(relationshipType);
            } else {
                // Gérer le cas où la ligne n'est pas correctement formée
                // Peut-être en enregistrant une erreur ou en ignorant la ligne
                currentValue = new Relationship(); // Réinitialiser à un objet vide
            }
        }

        return hasNext; // Retourner vrai si une ligne a été lue, sinon faux
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        // Retourner la clé actuelle (numéro de ligne)
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        // Retourner la valeur actuelle (objet Relationship)
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // Retourner le progrès de la lecture en utilisant celui du LineRecordReader
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        // Fermer le LineRecordReader lorsque le lecteur est terminé
        lineRecordReader.close();
    }
}
