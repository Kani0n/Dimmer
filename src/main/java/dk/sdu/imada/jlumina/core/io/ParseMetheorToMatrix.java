package dk.sdu.imada.jlumina.core.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import dk.sdu.imada.console.Variables;

public class ParseMetheorToMatrix {
    
    String score;
    ArrayList<String> samples;
    HashMap<String, HashMap<String, Float>> map;

    public ParseMetheorToMatrix(String score){
        this.score = score;
        this.samples = new ArrayList<>();
        this.map = new HashMap<>();
    }

    public void parse(String[] files){

        System.out.println("Parsing Metheor data...");
        for (String file : files) {

            String[] path = file.split("/");
            String name = path[path.length-1];
            name = name.substring(0, name.length()-4);

            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.strip();
                    String[] values = line.split("\t");
                    if (this.score.equals(Variables.PDR) 
                    || this.score.equals(Variables.MHL)
                    || this.score.equals(Variables.FDRP)
                    || this.score.equals(Variables.qFDRP)){
                        String chr = values[0];
                        String start = values[1];
                        float value = Float.parseFloat(values[3]);
                        String cpg = chr + ":" + start;
                        if (this.map.containsKey(cpg)) {
                            this.map.get(cpg).put(name, value);
                        } else {
                            HashMap<String, Float> temp = new HashMap<>();
                            temp.put(name, value);
                            this.map.put(cpg, temp);
                        }
                    } else if (this.score.equals(Variables.PM) || this.score.equals(Variables.ME) || this.score.equals(Variables.LPMD)) {
                        System.out.println("TODO. Pls use another score for now!");
                        return;
                    } else {
                        System.out.println("No Metheor-Score specified!");
                        return;
                    }
                }
                this.samples.add(name);
                br.close();
            } catch (IOException e) {
                System.out.println("could not read sample at location: " + file);
            }
        }
    }

    public void writeToCsv(String beta_path){
        try {
            FileWriter writer = new FileWriter(beta_path);
            writer.write(String.valueOf(this.score));
            for (String sample: this.samples) {
                writer.write(",");
                writer.write(sample);
            }
            writer.write("\n");
            for (String cpg: this.map.keySet()) {
                writer.write(cpg);
                for (String sample: this.samples) {
                    writer.write(",");
                    if (this.map.get(cpg).containsKey(sample)) {
                        writer.write(String.valueOf(this.map.get(cpg).get(sample)));
                    } else {
                        writer.write("NaN");
                    }
                }
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
