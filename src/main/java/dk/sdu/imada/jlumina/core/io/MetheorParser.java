package dk.sdu.imada.jlumina.core.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import dk.sdu.imada.console.Util;
import dk.sdu.imada.console.Variables;
import dk.sdu.imada.jlumina.core.primitives.CpG;
import dk.sdu.imada.jlumina.core.util.LoadingQueue;
import dk.sdu.imada.jlumina.core.util.MetheorTsvLoader;
import dk.sdu.imada.jlumina.core.util.QueueThread;

public class MetheorParser {
    
    private String path;
	private int numThreads = 1;

	private String[] samples;
	
	private ArrayList<String> samples_path;
	private ArrayList<String> errors;
	private ArrayList<String> warnings;
	private ArrayList<MetheorTsvLoader> reader_list;

	private ReadManifest manifest;
    private float[][] beta;

    /**
	 * A class to load .tsv files produced by metheor
	 * @param path leading to a sample annotation file
	 */
	public MetheorParser(String path){
		this.path = path;
		this.errors = new ArrayList<>();
		this.warnings = new ArrayList<>();
		this.samples_path = new ArrayList<>();
		this.samples = getSamples(path); //extract sample names from the sample file
	}

	/**
	 * extracts sample names from a annotation file
	 * @param path a path leading to a sample annotation file
	 * @return the sample names
	 */
	private String[] getSamples(String path){
		ArrayList<String> samples_list = new ArrayList<>();
		
		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = br.readLine();
			String[] header = line.split(",");
			int sample_column = -1;
			for(int i = 0; i < header.length; i++){
				if(header[i].equals(Variables.METHEOR_SAMPLE)){
					sample_column = i;
					break;
				}
			}
			while((line = br.readLine())!=null){
				samples_list.add(line.split(",")[sample_column].replace("\\","/"));
			}
			br.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		
		return samples_list.toArray(new String[samples_list.size()]);
	}

	/**
	 * loads .cov files specified by the constructor path
	 * @param numThreads number of threads used for parallelization
	 */
	public void load(int numThreads)  throws OutOfMemoryError{
		
		this.numThreads = numThreads;
		this.quickCheck(); //checks samples, sets samples_path variables
		System.out.println("Reading data...");
		this.readData(); //reads the data with multiple threads
		this.addReaderErrorsAndWarnings(); //Add reader errors and warnings to own list
		if(!this.check()){ //stops if there are errors
			return;
		}
		System.gc();
		System.out.println(Util.memoryLog());
		
		System.out.println("Merging and filtering...");
		this.mergeData(); //creates manifest file
		System.gc();
		System.out.println(Util.memoryLog());
		
		
		System.out.println("Creating beta-matrix...");
		this.initBeta(); //creates methylated/unmethylated matrices
		System.out.println("Created beta matrix with " + this.manifest.getCpgList().length + " CpGs\n");
		System.out.println("Succesfully loaded " + this.manifest.getCpgList().length + " CpGs\n");
		
		System.gc();
		System.out.println(Util.memoryLog());
	}

	/**
	 * checks existence, reading access and format correctness of the first line of all input files
	 * sets the actual path for every sample (can be total or in the same directory as the annotation file)
	 * @return true, if test is passed, else false
	 */
	public boolean quickCheck(){
		HashSet<String> path_set = new HashSet<>();
		for(String sample: this.samples){
			File sample_file = new File(sample);
			if(!sample_file.exists()){
				sample_file = new File(new File(this.path).getParentFile() + "/" + sample);
			}
			if(!sample_file.exists()){
				errors.add("Sample " + sample + " doesn't exist as full path or in the sample sheets directory.");
				continue;
			}
			String absolute_path = sample_file.getAbsolutePath();
			this.samples_path.add(absolute_path);
			if(path_set.contains(absolute_path)){
				errors.add("Duplicate sample: " + sample);
				continue;
			}
			path_set.add(absolute_path);
			if(!sample_file.canRead()){
				errors.add("No reading access: " + sample);
				continue;
			}
			MetheorTsvLoader readMetheorTsv = new MetheorTsvLoader(absolute_path);
			readMetheorTsv.quickCheck();
			if(!readMetheorTsv.check()){
				errors.addAll(readMetheorTsv.getErrors());
			}
		}
		return errors.size() == 0;
	}

	/**
	 * reads the data specified in the annotation file (path variable in the constructor) with multiple threads
	 * results get stored in hashmaps (chr -> position -> scores)
	 */
	private void readData()  throws OutOfMemoryError {
		
		Queue<MetheorTsvLoader> queue = new ConcurrentLinkedQueue<>(); // need to be read
		LoadingQueue<MetheorTsvLoader> loaded = new LoadingQueue<>(); // finished reading
		this.reader_list = new ArrayList<>(); //overall reader list
		
		//create reader objects
		for(String path : this.samples_path){
			MetheorTsvLoader reader = new MetheorTsvLoader(path);
			queue.add(reader);
			reader_list.add(reader);
		}
		
		//create threads and start them
		boolean overflow = false;
		
		
		ArrayList<QueueThread<MetheorTsvLoader>> threads = new ArrayList<>();
		for(int i = 0; i < numThreads; i++){
			QueueThread<MetheorTsvLoader> thread = new QueueThread<>(queue, loaded, i, overflow); 
			threads.add(thread);
			thread.start();
		}
		
		//monitor progress
		boolean done = false;
		try{
			while(!done){
				synchronized(loaded){
					loaded.wait();
					if(loaded.size() == reader_list.size()){
						done = true;
					}
					if(loaded.isOverflow()){
						throw new OutOfMemoryError();
					}
				}
			}
		}catch(InterruptedException e){
			e.printStackTrace();
		}
	}

	/**
	 * gets errors and warnings from the readers and adds them to own list
	 */
	private void addReaderErrorsAndWarnings(){
		for(MetheorTsvLoader reader : reader_list){
			if(reader.hasWarnings()){
				this.warnings.addAll(reader.getWarnings());
			}
			if(reader.hasErrors()){
				this.errors.addAll(reader.getErrors());
			}
		}
	}

	private void mergeData()  throws OutOfMemoryError {
		
		//get all chromosomes
		
		HashSet<String> all_chrs = new HashSet<>();
		
		for(MetheorTsvLoader reader: this.reader_list){
			
			HashMap<String,HashMap<Integer,Float>> reader_map = reader.getMap();
			
			all_chrs.addAll(reader_map.keySet());
		}
			
		//get all positions
		HashMap<String,HashMap<Integer,Short>> full_map = new HashMap<>();
		for(String chr : all_chrs){
			full_map.put(chr, new HashMap<Integer,Short>());
		}
		
		for(MetheorTsvLoader reader : this.reader_list){
			
			HashMap<String,HashMap<Integer,Float>> reader_map = reader.getMap();
			
			for(String chr : all_chrs){
				HashMap<Integer,Short> count_map = full_map.get(chr);
				HashMap<Integer,Float> pos_map = reader_map.get(chr);
				
				if(pos_map!=null){
					for(int pos : reader_map.get(chr).keySet()){
						count_map.put(pos, (short) (count_map.getOrDefault(pos, (short) 0) +  1)); //count appearance
					}
				}
			}
		}
		
		//init manifest 
		this.manifest = new ReadManifest();
		int n_CpGs = 0; //count CpGs to get size of the cpg list
		for(String chr: all_chrs){
			n_CpGs += full_map.get(chr).size();
		}
		CpG[] cpg_list = new CpG[n_CpGs];
		
		int i = 0; 	//fill cpg_list
		for(String chr: all_chrs){
			ArrayList<Integer> position_list = new ArrayList<>(full_map.get(chr).keySet());
			full_map.remove(chr);
			Collections.sort(position_list); //sorts the positions, in case they aren't sorted in the input files
			for(int position : position_list){
				cpg_list[i] = new CpG(chr, position);
				i++;
			}
		}
		this.manifest.setCpGList(cpg_list);
		
	}

	/**
	 * loads beta matrix
	 */
	 public void initBeta() throws OutOfMemoryError{
		//creates matrix for scores, stored data in the ReadMetheorTsv objects is deleted after every chromosome
		CpG[] cpg_list = this.manifest.getCpgList();
		this.beta = new float[cpg_list.length][];
		String current_chr = null; //keeps track of current chromosome for data deletion
		
		for(int i = 0; i < cpg_list.length; i++){
			
			CpG cpg = cpg_list[i];
			
			//delete data if chromosome changes
			if(cpg.getChromosome().equals(current_chr)){ 
				for(MetheorTsvLoader reader: this.reader_list){
					reader.getMap().remove(current_chr);
				}
				current_chr = cpg.getChromosome();
			}
			
			//init new row
			float[] beta_row = new float[this.reader_list.size()];
			
			//iterate through readers to fill rows
			for(int j = 0; j < this.reader_list.size(); j++){
				
				HashMap<Integer,Float> pos_map = this.reader_list.get(j).getMap().get(cpg.getChromosome());
				
				if(pos_map!=null){
					
					Float beta_value = pos_map.getOrDefault(cpg.getMapInfo(),(float)-1);
					if(beta_value>=0){
						beta_row[j] = beta_value;
					}
					else{
						beta_row[j] = Float.NaN;
					}	
					
				}
				else{
					beta_row[j] = Float.NaN;
				}
			}
			
			//add rows
			this.beta[i] = beta_row;
		}
		
		//delete data maps of the readers
		for(MetheorTsvLoader reader: this.reader_list){
			reader.setMap(null);
		}
	}

	public boolean check(){
		return this.errors.size() == 0;
	}
	
	public ArrayList<String> getErrors(){
		return this.errors;
	}

	public String errorLog(){
		return Util.errorLog(this.errors);
	}

	public float[][] getBeta(){
		return this.beta;
	}

	public ReadManifest getManifest(){
		return this.manifest;
	}

	public boolean hasWarnings(){
		return this.warnings.size()!=0;
	}

	public String warningLog(){
		return Util.warningLog(this.warnings);
	}

}
