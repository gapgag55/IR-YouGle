

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
			throws IOException {
		/*
		 * TODO: Your code here
		 *	 
		 */
		Pair<Long,Integer> posfre = new Pair<Long,Integer>(fc.position(), posting.getList().size());
		postingDict.put(posting.getTermId(),posfre);
		index.writePosting(fc, posting);
	}
	

	 /**
     * Pop next element if there is one, otherwise return null
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }
	
    
   
	
	/**
	 * Main method to start the indexing process.
	 * @param method		:Indexing method. "Basic" by default, but extra credit will be given for those
	 * 			who can implement variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname	:relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname	:relative path to the output directory to store index. You must not assume
	 * 			that this directory exist. If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException 
	{
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}
		
		   
		/* Get output directory*/
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}
		
		/*	TODO: delete all the files/sub folder under outdir
		 * 
		 */
		File[] files = outdir.listFiles();
		for(File file:files) {
			file.delete();
		}
		
		
		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}
		
		
		
		
		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block "+block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();
			
			//Bank added
			TreeMap<Integer,TreeSet<Integer>> bPostingList = new TreeMap<Integer,TreeSet<Integer>>();
			
			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				
				 // use pre-increment to ensure docID > 0
                int docId = ++docIdCounter;
                docDict.put(fileName, docId);
				
				
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * TODO: Your code here
						 *       For each term, build up a list of
						 *       documents in which the term occurs
						 */
						
						if(termDict.containsKey(token)) {
							
							if(bPostingList.containsKey(termDict.get(token))) {
								
								bPostingList.get(termDict.get(token)).add(docIdCounter);
							}else {
								
								TreeSet<Integer> docs = new TreeSet<Integer>();
								docs.add(docIdCounter);
								bPostingList.put(termDict.get(token), docs);
								
							}
							
						}else {
							wordIdCounter++;
							termDict.put(token,wordIdCounter);
							TreeSet<Integer> docs = new TreeSet<Integer>();
							docs.add(docIdCounter);
							bPostingList.put(wordIdCounter, docs);
						}
						
						

					}
				}
				reader.close();
			}
			

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}
			
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			FileChannel fc = bfc.getChannel();
			/*
			 * TODO: Your code here
			 *       Write all posting lists for all terms to file (bfc) 
			 */
			
			for(Integer termID : bPostingList.keySet()) {
				
				PostingList postingList = new PostingList(termID,new ArrayList<Integer>(bPostingList.get(termID)));
				index.writePosting(fc,postingList);
			}
			fc.close();
			bfc.close();
		}
	

		/* Required: output total number of files. */
		//System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			 
			/*
			 * TODO: Your code here
			 *       Combine blocks bf1 and bf2 into our combined file, mf
			 *       You will want to consider in what order to merge
			 *       the two blocks (based on term ID, perhaps?).
			 *       
			 */
			
			FileChannel fc1 = bf1.getChannel();
			FileChannel fc2 = bf2.getChannel();
			FileChannel fc3 = mf.getChannel();
			
			PostingList postingListB1 = index.readPosting(fc1);
			PostingList postingListB2 = index.readPosting(fc2);
			
			
			PostingList mgPostingList = null;
			
			
			
			while(postingListB1 != null && postingListB2 != null) {
			
				if(postingListB1.getTermId() < postingListB2.getTermId()) {
					mgPostingList = postingListB1;
					postingListB1 = index.readPosting(fc1);
				}else if(postingListB2.getTermId()<postingListB1.getTermId()) {
					mgPostingList = postingListB2;
					postingListB2 = index.readPosting(fc2);
					
				}else if(postingListB1.getTermId()==postingListB2.getTermId()) {
					
					List<Integer> mglist = mergeList(postingListB1,postingListB2);
					mgPostingList = new PostingList(postingListB1.getTermId(),mglist);
					postingListB1 = index.readPosting(fc1);
					postingListB2 = index.readPosting(fc2);
				}
				
				writePosting(fc3, mgPostingList);
			}
			while(postingListB1 != null) {
				
				writePosting(fc3, postingListB1);
				postingListB1 = index.readPosting(fc1);
			}
			
			while(postingListB2 != null) {
			
				writePosting(fc3, postingListB2);
				postingListB2 = index.readPosting(fc2);
			}
			
			
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}
		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
		
		return totalFileCount;
	}
	public static List<Integer> mergeList(PostingList postingListB1, PostingList postingListB2) {
		
		List<Integer> list1 = postingListB1.getList();
		List<Integer> list2 = postingListB2.getList();
		int listsize1 = list1.size();
		int listsize2 = list2.size();
		int count1 = 0;
		int count2 = 0;
		int temp1 = list1.remove(0);
		int temp2 = list2.remove(0);
		
		
		List<Integer> mgLists = new ArrayList<Integer>();
		//merging
		while(true) {				
			if(temp1<temp2) {
				mgLists.add(temp1);
				count1++;
				if(count1==listsize1)break;
				temp1 = list1.remove(0);
				
			}
			if(temp2<temp1) {
				mgLists.add(temp2);
				count2++;
				if(count2==listsize2)break;
				temp2 = list2.remove(0);
				
			}
			if(temp1==temp2) {
				mgLists.add(temp1);
				count1++;
				count2++;
				if(count1==listsize1)break;
				if(count2==listsize2)break;
				temp1 = list1.remove(0);
				temp2 = list2.remove(0);
				
			}
		}
		
		while(count1<listsize1) {
			mgLists.add(temp1);
			count1++;
			if(count1==listsize1)break;
			temp1 = list1.remove(0);
		}
		while(count2<listsize2) {
			mgLists.add(temp2);
			count2++;
			if(count2==listsize2)break;
			temp2 = list2.remove(0);
		}
		return mgLists;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];
		

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
