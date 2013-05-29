import java.sql.*;
import java.io.*;
import java.util.zip.*;
/*
 * Program na vytazeni souboru z databaze EEGERP a ulozeni do souboroveho
 * systemu dle scenaru a experimentu.
 */

/**
 *
 * @author rtryml
 */
public class EEGTransferCron {
    // pristupove udaje k databazi
    static String url; //= "jdbc:oracle:thin:@students.kiv.zcu.cz:1521:EEGERP";
    static String username; // = "EEGTEST" ; 
    static String password; // = "JPERGLER" ; 
    public static void main(String[] args){
        String aktPath;
        if(args.length == 0){
            File path = new File("");
            aktPath = aktPath = path.getAbsolutePath() + File.separator;
        }
        else{
            aktPath = args[0];
        }
        try{
            File path = new File("");
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path.getAbsolutePath()+ File.separator + "connectDB")));
            url = "jdbc:oracle:thin:@" + br.readLine() + ":" + br.readLine() + ":" +
                    br.readLine();
            username = br.readLine();
            password = br.readLine();
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
        // vytvoreni spojeni k databezi
        try{
            Connection conn = DriverManager.getConnection(url,username,password);
            transferFile(conn, aktPath);
            // ziskani metadat
            System.out.println("Create metadata.");
            createMetadata(conn, aktPath);
            System.out.println("Done");
            System.out.println("Transfer successful.");
            // zavreni spojeni na databazi
            conn.close();
        }
        catch(SQLException e){
            System.out.println("Error connecting to database.");
        }
    }
    
    // metoda pro presun souboru
    static public void transferFile(Connection conn, String aktPath){
        
        aktPath = aktPath + File.separator + "experiments" + File.separator;
        
        try{
            // pripraveny select pro vytazeni cisla experimentu, nazvu souboru a blob
            // z databaze
            String sqlScenario = "select title,scenario_id from scenario";
            PreparedStatement stmtScenario = conn.prepareStatement(sqlScenario);
            ResultSet resultSetScenario = stmtScenario.executeQuery();
            while(resultSetScenario.next()){
                String sqlExperiment = "select experiment_id from experiment where scenario_id = " 
                        + resultSetScenario.getString(2);
                PreparedStatement stmtExperiment = conn.prepareStatement(sqlExperiment);
                ResultSet resultSetExperiment = stmtExperiment.executeQuery();
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
                        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream
                            (part + "Data.zip"));
                        while(resultSetFile.next()){
                            ZipEntry entry = new ZipEntry(resultSetFile.getString(1));
                            zos.putNextEntry(entry);
                            byte[] buffer = new byte[1];
                            // pripojeni proudu binarnich dat z BLOB
                            InputStream is = resultSetFile.getBinaryStream(2);
                            // cteni z proudu dat, dokud je co cist
                            //System.out.println("Transfer file " + resultSetFile.getString(1) +
                            //    " to path " + f.getPath());
                            while (is.read(buffer) > 0 ){ 
                                zos.write( buffer ); 
                            }
                            is.close();
                        }
                        zos.close();
                    }
                    catch(IOException e){
                        System.out.println("Error the file transfer from database.");
                    }
                    
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

    static public void createMetadata(Connection conn, String aktPath){
        try {
            aktPath = aktPath + File.separator + "experiments" + File.separator;
            FileWriter fw;
            String pathTextAktExperiment;
            File pathFileAktExperiment;
            // ziskame data z experimentu
            String sql = "select experiment.*, scenario.title from experiment,"
                    + "scenario where experiment.scenario_id = scenario.scenario_id";
            PreparedStatement stmt;
            stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
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
                fw = new FileWriter(pathTextAktExperiment + "metadata.csv");
                // doplnit stazeni metadat experimentu
                fw.write("\"Experiment detail\"\n");
                fw.write("\"Beginning of Experiment\";" + 
                        prazdnyRetezec(resultSet.getString("START_TIME")) + "\n");
                fw.write("\"End of Experiment\";" + 
                        prazdnyRetezec(resultSet.getString("END_TIME")) + "\n");
                fw.write("\"Temperature[°C]\";" + 
                        prazdnyRetezec(resultSet.getString("TEMPERATURE")) + "\n");
                fw.write("\"Environment Note\";" + 
                        prazdnyRetezec(resultSet.getString("ENVIRONMENT_NOTE")) + "\n");
                fw.write("\"Tested subject\"\n");
                // zjisteni testovane osoby
                String sqlOsoba = "select gender, date_of_birth from person where "
                        + "person_id = " + resultSet.getString("SUBJECT_PERSON_ID");
                PreparedStatement stmtOsoba = conn.prepareStatement(sqlOsoba);
                ResultSet resultSetOsoba = stmtOsoba.executeQuery();
                resultSetOsoba.next();
                fw.write("\"Gender\";" + 
                        prazdnyRetezec(resultSetOsoba.getString(1)) + "\n");
                fw.write("\"Date of Birth\";"
                        + prazdnyRetezec(resultSetOsoba.getString(2)) + "\n");
                resultSetOsoba.close();
                stmtOsoba.close();
                // zjisteni hardware
                fw.write("\"Used hardware\"\n");
                String sqlHardware = "select description from electrode_system "
                        + "where electrode_system_id = (select electrode_system_id "
                        + "from electrode_conf where electrode_conf_id = " 
                        + resultSet.getString("ELECTRODE_CONF_ID") + ")";
                PreparedStatement stmtHardware = conn.prepareStatement(sqlHardware);
                ResultSet resultSetHardware = stmtHardware.executeQuery();
                resultSetHardware.next();
                fw.write("\"Description\";"
                        + prazdnyRetezec(resultSetHardware.getString(1)) + "\n");
                resultSetHardware.close();
                stmtHardware.close();
                // stazeni seznamu souboru a jejich popis
                String sqlFile = "select filename, description from data_file where "
                    + "experiment_id = " + resultSet.getString("EXPERIMENT_ID");
                PreparedStatement stmtFile;
                stmtFile = conn.prepareStatement(sqlFile);
                ResultSet resultSetFile = stmtFile.executeQuery();
                fw.write("\"File name\";\"Description\"\n");
                while(resultSetFile.next()){
                    fw.write(prazdnyRetezec(resultSetFile.getString(1)) + ";" + 
                    prazdnyRetezec(resultSetFile.getString(2)) + "\n");
                }
                resultSetFile.close();
                stmtFile.close();
                fw.close();
            }
            resultSet.close();
            stmt.close();
        } 
        catch (SQLException e) {
            System.out.println("Create metadata - SQL Error");
        }
        catch (IOException e){
            System.out.println("Create metadata - File create error");
            System.exit(1);
        }
    }
    
    static public String prazdnyRetezec(String retezec){
        if(retezec == null){
            return "";
        }
        return "\"" + retezec + "\"";
    }
}