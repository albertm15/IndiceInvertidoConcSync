
package eps.scp;
import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class RecorrerFicheroDeLoad implements Runnable{

    private File file;
    private Map<String, HashSet<Location>> Hash;
    public RecorrerFicheroDeLoad(File file, Map<String, HashSet <Location>> Hash){
        this.file = file;
        this.Hash = Hash;
    }

    @Override
    public void run() {
        try {
            FileReader input = new FileReader(file);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try {
                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    HashSet<Location> locationsList = new HashSet<Location>();
                    // Descomponemos la línea leída en su clave (word) y las ubicaciones
                    String[] fields = keyLine.split("\t");
                    String word = fields[0];
                    String[] locations = fields[1].split(", ");
                    // Recorremos los offsets para esta clave y los añadimos al HashMap
                    for (int i = 0; i < locations.length; i++)
                    {
                        String[] location = locations[i].substring(1, locations[i].length()-1).split(",");
                        int fileId = Integer.parseInt(location[0]);
                        int line = Integer.parseInt(location[1]);
                        locationsList.add(new Location(fileId,line));
                    }
                    InvertedIndex.addLocationLoad(word, locationsList);
                }
            } catch (IOException e) {
                System.err.println("Error reading Index file");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Index file");
            e.printStackTrace();
        }
        try {
            LoadInvertedIndex.barrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
}
