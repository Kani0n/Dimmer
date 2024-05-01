package dk.sdu.imada.jlumina.core.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class ReadMetheorTsv implements Runnable{

    private String path;
	private String sample;
	private HashMap<String,HashMap<Integer,Float>> map; //used for data storage (chr -> positions -> score)
	private int n_CpGs;
	
	private ArrayList<String> errors;
	private ArrayList<String> warnings;

    public ReadMetheorTsv(String path){
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
            int start = Integer.parseInt(splitted[1]);
            float score = Float.parseFloat(splitted[3]);
            String cpg = chr + ":" + start;
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

					int start = Integer.parseInt(splitted[1]);
                    Float score = Float.parseFloat(splitted[3]);

                    HashMap<Integer,Float> chr_map = this.map.get(chr);
                    if(chr_map == null){
                        chr_map = new HashMap<Integer,Float>();
                        this.map.put(chr, chr_map);
                    }
                    
                    Float value = chr_map.get(start);
                    if(value != null){
                        warnings.add("Duplicate position " + chr + ":" + start + " in sample " + this.sample +" was overwritten.");
                        this.n_CpGs--;
                    }
                    chr_map.put(start, score);
                    this.n_CpGs++;	
		
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

	public HashMap<String,HashMap<Integer,Float>> getMap(){
		return this.map;
	}

	public void setMap(HashMap<String,HashMap<Integer,Float>> map){
		this.map = map;
	}

	public int hashCode(){
		return this.path.hashCode();
	}

}
