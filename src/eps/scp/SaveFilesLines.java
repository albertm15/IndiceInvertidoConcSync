/* ---------------------------------------------------------------
Práctica 3.
Código fuente : IndiceInvertidoConcPRA3
Grado Informática
49381774S Albert Martín López.
49380060A Pau Barahona Setó.
--------------------------------------------------------------- */

package eps.scp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class SaveFilesLines implements Runnable{
    private final String DFileLinesName = "FilesLinesContent";
    private String outputDirectory;
    private Map<Location, String> IndexFilesLines;
    private CountDownLatch latch;
    public SaveFilesLines(String indexDirectory, Map<Location, String> IndexFilesLines, CountDownLatch latch){
        this.outputDirectory = indexDirectory;
        this.IndexFilesLines = IndexFilesLines;
        this.latch = latch;
    }
    @Override
    public void run() {
        try {
            File KeyFile = new File(outputDirectory + "/" + DFileLinesName);
            FileWriter fw = new FileWriter(KeyFile);
            BufferedWriter bw = new BufferedWriter(fw);
            Set<Map.Entry<Location, String>> keySet = IndexFilesLines.entrySet();
            Iterator keyIterator = keySet.iterator();

            while (keyIterator.hasNext() )
            {
                Map.Entry<Location, String> entry = (Map.Entry<Location, String>) keyIterator.next();
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
            bw.close(); // Cerramos el fichero.
        } catch (IOException e) {
            System.err.println("Error creating FilesLines contents file: " + outputDirectory + DFileLinesName + "\n");
            e.printStackTrace();
            System.exit(-1);
        }
        this.latch.countDown();
    }
}
