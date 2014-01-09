package udf;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class BinLaden extends EvalFunc<Boolean> {
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return false;
		try{
			String str = (String)input.get(0);
			if(str == null) return false;
			return str.toLowerCase().contains("bin laden");
		}catch(Exception e){
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}