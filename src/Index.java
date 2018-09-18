import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict = new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict = new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue = new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	/*
	 * Write a posting list to the given file You should record the file position of
	 * this posting list so that you can read it back during retrieval
	 *
	 */
	private static void writePosting(FileChannel fc, PostingList posting) throws IOException {
		/*
		 * TODO: Your code here
		 *
		 */
		List<Integer> list = posting.getList();
		Pair<Long, Integer> docFreq = new Pair<Long, Integer>((long) fc.position(), list.size());
		postingDict.put(posting.getTermId(), docFreq);

		// Call Indexing Writer
		index.writePosting(fc, posting);
	}

	/**
	 * Pop next element if there is one, otherwise return null
	 *
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
	 *
	 * @param method        :Indexing method. "Basic" by default, but extra credit
	 *                      will be given for those who can implement variable byte
	 *                      (VB) or Gamma index compression algorithm
	 * @param dataDirname   :relative path to the dataset root directory. E.g.
	 *                      "./datasets/small"
	 * @param outputDirname :relative path to the output directory to store index.
	 *                      You must not assume that this directory exist. If it
	 *                      does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException {
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}

		/* Get output directory */
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}

		/*
		 * TODO: delete all the files/sub folder under outdir
		 *
		 */
		deleteDir(outdir);

		// Added by [Kopkap]
		// Pupose: to add the couple of termId, and docId (termId, docId)
		Map<Integer, ArrayList<Integer>> postingTemp = new TreeMap<Integer, ArrayList<Integer>>();

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block " + block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();

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
						 * TODO: Your code here For each term, build up a list of documents in which the
						 * term occurs
						 */

						// Is that token contains in termDict?
						if (termDict.containsKey(token)) {
							// Get termId from that token
							int termId = termDict.get(token);

							// Is that termId contains in postingTemp? -> No
							// Because postingTemp will re-initiate each block
							if (postingTemp.get(termId) == null) {

								// Store termId to postingTerm
								postingTemp.put(termId, new ArrayList());
								postingTemp.get(termId).add(docId);
							}

							// Is that termId contains in postingTemp? -> Yes
							if (postingTemp.get(termId) != null) {

								// If postingTemp does not contain docId
								if (!postingTemp.get(termId).contains(docId)) {

									// Fire them up
									postingTemp.get(termId).add(docId);
								}
							}
						}

						if (!termDict.containsKey(token)) {
							int termId = ++wordIdCounter;
							termDict.put(token, termId);
							// System.out.println("TermId: " + termId + "\t" + token);
							postingTemp.put(termId, new ArrayList());
							postingTemp.get(termId).add(docId);
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

			/*
			 * TODO: Your code here Write all posting lists for all terms to file (bfc)
			 */
			FileChannel fc = bfc.getChannel();

			for (int keyTerm : postingTemp.keySet()) {
				// System.out.println("TermId: " + keyTerm + "\t" + postingTemp.get(keyTerm));
				PostingList posting = new PostingList(keyTerm, postingTemp.get(keyTerm));
				writePosting(fc, posting);
			}

			// Reset postingTemp per block
			postingTemp = new TreeMap<Integer, ArrayList<Integer>>();
			bfc.close();
			// break;
		}

		/* Required: output total number of files. */
		// System.out.println("Total Files Indexed: "+totalFileCount);

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
			 * TODO: Your code here Combine blocks bf1 and bf2 into our combined file, mf
			 * You will want to consider in what order to merge the two blocks (based on
			 * term ID, perhaps?).
			 *
			 */
			ArrayList<PostingList> APosting, BPosting;
			PostingList AReader, BReader;
			FileChannel fc1, fc2;

			APosting = new ArrayList<PostingList>();
			BPosting = new ArrayList<PostingList>();

			fc1 = bf1.getChannel();
			fc2 = bf2.getChannel();

			AReader = index.readPosting(fc1);
			BReader = index.readPosting(fc2);

			// Get Postings of two files
			while (AReader != null || BReader != null) {
				if (AReader != null)
					APosting.add(AReader);

				if (BReader != null)
					BPosting.add(BReader);

				AReader = index.readPosting(fc1);
				BReader = index.readPosting(fc2);
			}

			// Merge
			ArrayList<PostingList> combinePosting = new ArrayList<PostingList>();
			ArrayList<Integer> docs;
			PostingList tempPostingA, tempPostingB;
			int termIdA, termIdB;

			int sizeA = APosting.size();
			int sizeB = BPosting.size();
			int indexA = 0;
			int indexB = 0;

			while (indexA < sizeA && indexB < sizeB) {
				tempPostingA = APosting.get(indexA);
				tempPostingB = BPosting.get(indexB);

				termIdA = tempPostingA.getTermId();
				termIdB = tempPostingB.getTermId();

				if (termIdA == termIdB) {
					// add docs behind
					// Create new ArrayList<Posting>
					docs = new ArrayList<Integer>();

					for (int docId : tempPostingA.getList()) {
						docs.add(docId);
					}

					for (int docId : tempPostingB.getList()) {
						docs.add(docId);
					}

					Collections.sort(docs);
					combinePosting.add(new PostingList(termIdA, docs));
					++indexA;
					++indexB;
				}

				if (termIdA < termIdB) {
					combinePosting.add(tempPostingA);
					++indexA;
				}

				if (termIdB < termIdA) {
					combinePosting.add(tempPostingB);
					++indexB;
				}
			}

			if (indexB >= sizeB) {
				while (indexA < sizeA) {
					combinePosting.add(APosting.get(indexA));
					++indexA;
				}
			}

			if (indexA >= sizeA) {
				while (indexB < sizeB) {
					combinePosting.add(BPosting.get(indexB));
					++indexB;
				}
			}

			// Print Combine Posting
			// for (PostingList posting : combinePosting) {
			// System.out.println("termId: " + posting.getTermId() + "\tDocs: " +
			// posting.getList());
			// }
			// System.out.println("---------- END COMBINDED POSTING ----------");

			// WritePosting to combfile
			FileChannel fc3 = mf.getChannel();

			for (PostingList postingList : combinePosting)
				writePosting(fc3, postingList);

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

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			// System.out.println(term + "\t" + termDict.get(term) + "\n");
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(
					termId + "\t" + postingDict.get(termId).getFirst() + "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();

		return totalFileCount;
	}

	// Added by [Kopkap]
	// Delete recursively
	public static void deleteDir(File file) {
		for (File subFile : file.listFiles()) {
			if (subFile.isDirectory()) {
				deleteDir(subFile);
			}
			subFile.delete();
		}
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
