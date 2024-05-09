package dk.sdu.imada.jlumina.core.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.ojalgo.optimisation.Variable;

import dk.sdu.imada.console.Variables;

public class MetheorTsvLoader implements Runnable{

    private String path;
	private String sample;
	private HashMap<String,HashMap<Integer,ArrayList<Float>>> map; //used for data storage (chr -> positions -> scores)
	private int n_CpGs;

	private String score;
	
	private ArrayList<String> errors;
	private ArrayList<String> warnings;

    public MetheorTsvLoader(String path, String score){
		this.score = score;
		this.errors = new ArrayList<>();
		this.warnings = new ArrayList<>();
		this.path = path.replace("\\","/");
		this.map = new HashMap<>();
        String sample_name = this.path.split("/")[this.path.split("/").length-1];
		this.sample = sample_name.substring(0, sample_name.length()-4);
		this.n_CpGs = 0;
	}

    /**
	 * checks format of first line
	 */
	public void quickCheck() {
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = br.readLine();
			String[] splitted = line.split("\t");
			String chr = splitted[0];
			if (this.score.equals(Variables.PDR) || this.score.equals(Variables.MHL) || this.score.equals(Variables.FDRP) || this.score.equals(Variables.qFDRP)) {
				int start_1 = Integer.parseInt(splitted[1]);
				int start_2 = Integer.parseInt(splitted[2]);
				int start_3 = Integer.parseInt(splitted[3]);
				int start_4 = Integer.parseInt(splitted[4]);
				float value = Float.parseFloat(splitted[5]);
				String cpg_1 = chr + ":" + start_1;
				String cpg_2 = chr + ":" + start_2;
				String cpg_3 = chr + ":" + start_3;
				String cpg_4 = chr + ":" + start_4;
			} else if (this.score.equals(Variables.PM)|| this.score.equals(Variables.ME)) {
				int start = Integer.parseInt(splitted[1]);
				float value = Float.parseFloat(splitted[3]);
				String cpg = chr + ":" + start;
			} else {
				errors.add("Unknown metheor score: " + score);
			}
			br.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		catch(IndexOutOfBoundsException e){
			errors.add("Column number error in sample " + sample + " line " + 1);
		}
		catch(NumberFormatException e){
			errors.add("Number format error in sample " + sample + " line " + 1);
		}
	}

    /**
	 * read data, data gets stored in a hashmap (chr -> positions -> score)
	 */
	public void read() throws OutOfMemoryError{
		try{
			
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = null;
			int line_counter = 0;
			
			while((line = br.readLine())!=null){
				
				line_counter++;
				
				try{
					
					String[] splitted = line.split("\t");
					String chr = splitted[0];

					if (this.score.equals(Variables.PDR) || this.score.equals(Variables.MHL) || this.score.equals(Variables.FDRP) || this.score.equals(Variables.qFDRP)) {

						int start = Integer.parseInt(splitted[1]);
						Float s = Float.parseFloat(splitted[3]);

						HashMap<Integer, ArrayList<Float>> chr_map = this.map.get(chr);
						if(chr_map == null){
							chr_map = new HashMap<Integer, ArrayList<Float>>();
							this.map.put(chr, chr_map);
						}
						
						ArrayList<Float> values = chr_map.get(start);
						if(values != null){
							values.add(s);
							this.n_CpGs--;
						}
						chr_map.put(start, new ArrayList<>(Arrays.asList(s)));
                    	this.n_CpGs++;	

					} else if (this.score.equals(Variables.PM)|| this.score.equals(Variables.ME)) {
						Float s = Float.parseFloat(splitted[5]);

						HashMap<Integer, ArrayList<Float>> chr_map = this.map.get(chr);
						if(chr_map == null){
							chr_map = new HashMap<Integer, ArrayList<Float>>();
							this.map.put(chr, chr_map);
						}
						
						for (int i=1; i<=4; i++) {
							int start = Integer.parseInt(splitted[i]);
							ArrayList<Float> values = chr_map.get(start);
							if(values != null){
								values.add(s);
								this.n_CpGs--;
							}
							chr_map.put(start, new ArrayList<>(Arrays.asList(s)));
							this.n_CpGs++;
						}
						
					} else {
						errors.add("Unknown metheor score: " + score);
					}
		
				}
				catch(NumberFormatException e){
					errors.add("Number format error while reading sample " + this.sample +  " line " + line_counter);
					br.close();
					return;
				}
				catch(IndexOutOfBoundsException e){
					errors.add("Column number error while reading sample " + this.sample + " line " + line_counter);
                    br.close();
                    return;
				}

			}
			br.close();
			
		}catch(IOException e){
			e.printStackTrace();
		}


	}

	public String toString(){
		if(n_CpGs != 0){
			return this.sample + " Loaded CpGs: " + n_CpGs;
		}
		else{
			return this.sample;
		}

	}

    public boolean check(){
		return this.errors.size() == 0;
	}

    public ArrayList<String> getErrors(){
		return this.errors;
	}

    public void run() throws OutOfMemoryError{
		read();
	}
	
	public ArrayList<String> getWarnings(){
		return this.warnings;
	}

    public boolean hasErrors(){
		return this.errors.size()!=0;
	}

	public boolean hasWarnings(){
		return this.warnings.size()!=0;
	}

	public HashMap<String,HashMap<Integer, ArrayList<Float>>> getMap(){
		return this.map;
	}

	public void setMap(HashMap<String,HashMap<Integer, ArrayList<Float>>> map){
		this.map = map;
	}

	public int hashCode(){
		return this.path.hashCode();
	}

}
