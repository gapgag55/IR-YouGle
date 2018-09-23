import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		PostingList rePostingList = null;
		int intSize = 4;
		ByteBuffer buf = ByteBuffer.allocate(intSize*2);
		try {
			if(fc.read(buf)!= -1){
				buf.flip();
				List<Integer> postingLists = new ArrayList<Integer>();
				int termID = buf.getInt();
				int size = buf.getInt();
				
				buf = ByteBuffer.allocate(intSize*size);
				fc.read(buf);
				buf.flip();
				for(int i = 0;i<size;i++) {
					postingLists.add(buf.getInt());
					
				}
				rePostingList = new PostingList(termID,postingLists);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return rePostingList;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		int intSize = 4;
		List<Integer> lists = p.getList();
		int lengthOfBuffer = 2+lists.size();
		
		ByteBuffer buf = ByteBuffer.allocate(intSize*lengthOfBuffer);
		buf.putInt(p.getTermId());
		buf.putInt(lists.size());
		for(Integer doc : lists) {
			buf.putInt(doc);
		}
		
		buf.flip();
		try {
			fc.write(buf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}