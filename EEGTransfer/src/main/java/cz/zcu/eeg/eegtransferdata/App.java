package cz.zcu.eeg.eegtransferdata;

import java.sql.*;
import java.io.*;
import java.util.zip.*;
/*
 * Program na vytazeni souboru z databaze EEGERP a ulozeni do souboroveho
 * systemu dle scenaru a experimentu.
 */

/**
 *
 * @author Roman Tryml
 */
public class App {
    // deklarace promenych pro pristupove udaje k databazi
    static String url; //= "jdbc:oracle:thin:@students.kiv.zcu.cz:1521:EEGERP";
    static String username; // = "EEGTEST" ; 
    static String password; // = "JPERGLER" ; 
    public static void main(String[] args){
        String aktPath;
        // parametr prikazove radky jako umisteni stahovanych dat 
        // pokud je prazdny, bere se aktualni adresar 
        if(args.length == 0){
            File path = new File("");
            aktPath = aktPath = path.getAbsolutePath() + File.separator;
        }
        else{
            aktPath = args[0];
        }
        try{
            // cteni pristupovych udaju k databazi z konfiguracniho souboru connectDB
            File path = new File("");
            // Otevreni vstupniho proudu dat ze souboru
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path.getAbsolutePath()+ File.separator + "connectDB")));
            url = "jdbc:oracle:thin:@" + br.readLine() + ":" + br.readLine() + ":" +
                    br.readLine();
            username = br.readLine();
            password = br.readLine();
            // zavreni vstupniho proudu dat po nacteni pristupovych udaju k databazi 
            br.close();
        }
        catch(IOException e){
            System.out.println("Error reading data access to the database.");
        }
        try{
            // nacteni driveru k databazi
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException e){
            System.out.println("Error initializing database driver.");
        }
        try{
            // vytvoreni spojeni k databazi
            Connection conn = DriverManager.getConnection(url,username,password);
            // spusteni metody pro stahovani souboru z databaze
            transferFile(conn, aktPath);
            // spusteni metody pro ziskani metadat
            createMetadata(conn, aktPath);
            System.out.println("Transfer successful.");
            // zavreni spojeni na databazi
            conn.close();
        }
        catch(SQLException e){
            System.out.println("Error connecting to database.");
        }
    }
    
    // metoda pro presun souboru
    // parametry jsou promenne tridy Connection (spojeni na DB) a
    // tridy String (aktualni adresar, kam se budou stahovat soubory)
    static public void transferFile(Connection conn, String aktPath){
        // zalozeni slozky experiment
        aktPath = aktPath + File.separator + "experiments" + File.separator;
        
        try{
            // pripraveny select pro vytazeni cisla experimentu, nazvu souboru a blob
            // z databaze
            String sqlScenario = "select title,scenario_id from scenario";
            PreparedStatement stmtScenario = conn.prepareStatement(sqlScenario);
            ResultSet resultSetScenario = stmtScenario.executeQuery();
            // cyklus prochazi scenare
            while(resultSetScenario.next()){
                String sqlExperiment = "select experiment_id from experiment where scenario_id = " 
                        + resultSetScenario.getString(2);
                PreparedStatement stmtExperiment = conn.prepareStatement(sqlExperiment);
                ResultSet resultSetExperiment = stmtExperiment.executeQuery();
                // cyklus prochazi prislusne experimenty k danemu scenari
                while(resultSetExperiment.next()){
                    String sqlFile = "select filename, file_content from data_file where "
                            + "experiment_id = " + resultSetExperiment.getString(1);
                    PreparedStatement stmtFile = conn.prepareStatement(sqlFile);
                    ResultSet resultSetFile = stmtFile.executeQuery();
                    // nyni jsou znamy komplet informace o slozce
                    // sestavime cestu
                    String part = resultSetScenario.getString(1) + File.separator +
                            resultSetExperiment.getString(1) + File.separator;
                    // kontrola na "bile znaky" resi problem s neplatnymi nazvy 
                    // slozek a souboru, pokud by byl na konci bily znak
                    part = part.replace(" ", "_");
                    part = aktPath + part;
                    File f = new File(part);
                    // jestlize slozka jeste neexistuje, vytvori se
                    if (!f.exists()){
                        f.mkdirs();
                    }
                    try{
                        // vytvoreni zip streamu
                        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream
                            (part + "Data.zip"));
                        // cyklus prochazi vsechny soubory prislusne k danemu experimentu
                        while(resultSetFile.next()){
                            // pridani souboru do zipu archivu
                            ZipEntry entry = new ZipEntry(resultSetFile.getString(1));
                            // pripojeni zip archivu do zip streamu 
                            zos.putNextEntry(entry);
                            byte[] buffer = new byte[1];
                            // pripojeni proudu binarnich dat z BLOB
                            InputStream is = resultSetFile.getBinaryStream(2);
                            // cteni z proudu dat, dokud je co cist
                            System.out.println("Transfer file " + resultSetFile.getString(1) +
                                " to path " + f.getPath());
                            while (is.read(buffer) > 0 ){ 
                                zos.write( buffer ); 
                            }
                            // zavreni input streamu
                            is.close();
                        }
                        // zavreni zip streamu
                        zos.close();
                    }
                    catch(IOException e){
                        System.out.println("Error the file transfer from database.");
                    }
                    // zavreni resutSetu a statementu
                    resultSetFile.close();
                    stmtFile.close();
                }
                resultSetExperiment.close();
                stmtExperiment.close();
            }
            resultSetScenario.close();
            stmtScenario.close();
            System.out.println("Transfer all data done.");
        }
        catch (SQLException e) {
            System.out.println("Transfer file - SQL Error.");
        } 
    }

    // metoda pro stahovani metadat
    // parametry jsou promenne tridy Connection (spojeni na DB) a
    // tridy String (aktualni adresar, ka se budou stahovat soubory)
    static public void createMetadata(Connection conn, String aktPath){
        try {
            aktPath = aktPath + File.separator + "experiments" + File.separator;
            FileWriter fw;
            String pathTextAktExperiment;
            File pathFileAktExperiment;
            // ziskame data z experimentu
            // upravou tohoto retezce lze nastavit, jake informace do metadatoveho 
            // souboru pozadujeme
            String sql = "select experiment.*, scenario.title from experiment,"
                    + "scenario where experiment.scenario_id = scenario.scenario_id";
            PreparedStatement stmt;
            stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            System.out.println("Create metadata.");
            // cyklus prochazi jednotlive radky selectu
            while(resultSet.next()){ 
                String part = resultSet.getString("TITLE") + 
                        File.separator + resultSet.getString("EXPERIMENT_ID") + File.separator;
                // kontrola na "bile znaky" resi problem s neplatnymi nazvy 
                // slozek a souboru, pokud by byl na konci bily znak
                part = part.replace(" ", "_");
                pathTextAktExperiment = aktPath + part;
                pathFileAktExperiment = new File(pathTextAktExperiment);
                if (!pathFileAktExperiment.exists()){
                    pathFileAktExperiment.mkdirs();
                }
                // zapisuje jednotlive polozky do souboru a oddelute znakem '|'
                fw = new FileWriter(pathTextAktExperiment + "metadata.csv");
                for(int i = 1; i < resultSet.getMetaData().getColumnCount(); i++){
                    fw.write(resultSet.getString(i) + "|\n");
                }
                // zavreni otevreneho souboru
                fw.close();
            }
            System.out.println("Done");
        } 
        catch (SQLException e) {
            System.out.println("Create metadata - SQL Error");
        }
        catch (IOException e){
            System.out.println("Create metadata - File create error");
            System.exit(1);
        }
    }
}
