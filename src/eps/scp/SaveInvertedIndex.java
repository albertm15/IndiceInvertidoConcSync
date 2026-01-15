
package eps.scp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class SaveInvertedIndex implements Runnable{
    private final int DIndexMaxNumberOfFiles = 200;
    private final int DIndexMinNumberOfFiles = 2;
    private final int DKeysByFileIndex = 1000;
    private final String DIndexFilePrefix = "IndexFile";
    private String outputDirectory;
    private Map<String, HashSet<Location>> Hash;
    private CountDownLatch latch;
    public SaveInvertedIndex(String indexDirectory, Map<String, HashSet <Location>> Hash, CountDownLatch latch){
        this.outputDirectory = indexDirectory;
        this.Hash = Hash;
        this.latch = latch;
    }
    @Override
    public void run() {
        saveInvertedIndex(outputDirectory);
        this.latch.countDown();
    }

    public void saveInvertedIndex(String outputDirectory){
        int numberOfFiles, remainingFiles;
        long remainingKeys=0, keysByFile=0;
        String key="";
        Charset utf8 = StandardCharsets.UTF_8;
        Set<String> keySet = Hash.keySet();

        numberOfFiles = keySet.size()/DKeysByFileIndex;
        // Calculamos el número de ficheros a crear en función del número de claves que hay en el hash.
        if (numberOfFiles>DIndexMaxNumberOfFiles)
            numberOfFiles = DIndexMaxNumberOfFiles;
        if (numberOfFiles<DIndexMinNumberOfFiles)
            numberOfFiles = DIndexMinNumberOfFiles;

        Iterator keyIterator = keySet.iterator();
        remainingKeys =  keySet.size();
        remainingFiles = numberOfFiles;
        // Bucle para recorrer los ficheros de indice a crear.
        int actualTaskNumber = 0;
        for (int f=1;f<=numberOfFiles;f++)
        {

            try {
                File KeyFile = new File(outputDirectory +"/"+ DIndexFilePrefix + String.format("%03d", f));
                FileWriter fw = new FileWriter(KeyFile);
                BufferedWriter bw = new BufferedWriter(fw);
                // Calculamos el número de claves a guardar en este fichero.
                keysByFile =  remainingKeys / remainingFiles;
                remainingKeys -= keysByFile;
                // Recorremos las claves correspondientes a este fichero.
                while (keyIterator.hasNext() && keysByFile>0) {
                    key = (String) keyIterator.next();
                    saveIndexKey(key,bw);  // Salvamos la clave al fichero.
                    keysByFile--;
                }
                bw.close(); // Cerramos el fichero.
                remainingFiles--;
            } catch (IOException e) {
                System.err.println("Error creating Index file " + outputDirectory + "/IndexFile" + f);
                e.printStackTrace();
                System.exit(-1);
            }

        }
    }

    public void saveIndexKey(String key, BufferedWriter bw)
    {
        try {
           HashSet<Location> locations = Hash.get(key);
            // Creamos un string con todos los offsets separados por una coma.
            //String joined1 = StringUtils.join(locations, ";");
            String joined = String.join(";",locations.toString());
            bw.write(key+"\t");
            bw.write(joined.substring(1,joined.length()-1)+"\n");
        } catch (IOException e) {
            System.err.println("Error writing Index file");
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
