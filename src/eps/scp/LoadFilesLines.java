/* ---------------------------------------------------------------
Práctica 3.
Código fuente : IndiceInvertidoConcPRA3
Grado Informática
49381774S Albert Martín López.
49380060A Pau Barahona Setó.
--------------------------------------------------------------- */

package eps.scp;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Phaser;

public class LoadFilesLines implements Runnable{
    private final String DFileLinesName = "FilesLinesContent";
    private String inputDirectory;
    private Map<Location, String> IndexFilesLines;
    private Phaser phaser;
    public LoadFilesLines(String indexDirectory, Map<Location, String> IndexFilesLines, Phaser phaser){
        this.inputDirectory = indexDirectory;
        this.IndexFilesLines = IndexFilesLines;
        this.phaser = phaser;
    }
    @Override
    public void run() {

        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFileLinesName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try
            {
                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (Location) y la linea de texto correspondiente
                    String[] fields = keyLine.split("\t");
                    String[] location = fields[0].substring(1, fields[0].length()-1).split(",");
                    int fileId = Integer.parseInt(location[0]);
                    int line = Integer.parseInt(location[1]);
                    fields[0]="";
                    String textLine = String.join("", fields);
                    IndexFilesLines.put(new Location(fileId,line),textLine);
                }
                bufRead.close();

            } catch (IOException e) {
                System.err.println("Error reading Files Ids");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Files Ids file");
            e.printStackTrace();
        }
        this.phaser.arriveAndDeregister();
    }
}
