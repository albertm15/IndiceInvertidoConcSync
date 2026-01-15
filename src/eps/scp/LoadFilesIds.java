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

public class LoadFilesIds implements Runnable{
    private final String DFilesIdsName = "FilesIds";
    private String inputDirectory;
    private Map<Integer,String> Files;
    private Phaser phaser;
    public LoadFilesIds(String indexDirectory, Map<Integer,String> Files, Phaser phaser){
        this.inputDirectory = indexDirectory;
        this.Files = Files;
        this.phaser = phaser;
    }
    @Override
    public void run() {

        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFilesIdsName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try {

                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (File Id) y la ruta del fichero.
                    String[] fields = keyLine.split("\t");
                    int fileId = Integer.parseInt(fields[0]);
                    fields[0]="";
                    String filePath = String.join("", fields);
                    Files.put(fileId, filePath);
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
