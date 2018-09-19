import java.nio.channels.FileChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		try {
			if (fc.position() < fc.size()) {
				// Get 2 Buffers first (termId, docFreq) from the blockFile
				// Each buffer has 4 bytes (Integer Size = 4 bytes) x 2 ea
				ByteBuffer Buffers = ByteBuffer.allocate(2 * 4);

				// Get data channel to Buffers
				fc.read(Buffers);

				// Get termId at the byte position
				int termId  = Buffers.getInt(0);

				// Get docFreq at the byte position at 2
				int docFreq = Buffers.getInt(4);

				// Get docIds by docFreq * 4 bytes
				ArrayList<Integer> postings = new ArrayList<Integer>(docFreq);
				ByteBuffer docs = ByteBuffer.allocate(docFreq * 4);

				// Now, buffer only has the docIds
				// fc.read() above remove termId and docFreq already.
				fc.read(docs);

				// Read all docIds
				for(int i = 0; i < docFreq; i++)
					postings.add(docs.getInt(i * 4));

				return new PostingList(termId, postings);
			}
		} catch(IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		int termId = p.getTermId();
		List<Integer> postings = p.getList();
		int docFreq = postings.size();
		int postingSize = (2 * 4) + (docFreq * 4);

		try {
			// 2 * 4 = 2 variables (termId, docFreq) will allocate each for 4 bytes, thus = 8 bytes
			// docFreq * 4 = each for 4 bytes allocated
			ByteBuffer bf = ByteBuffer.allocate(postingSize);
			bf.putInt(termId);
			bf.putInt(docFreq);

			for(int i = 0; i < docFreq; i++)
				bf.putInt(postings.get(i));

			bf.flip();
			fc.write(bf);

		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}