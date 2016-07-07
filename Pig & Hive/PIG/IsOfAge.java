package com.test.weather;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.parser.AliasMasker.input_clause_return;

	public class IsOfAge extends FilterFunc{
	@Override
	public Boolean exec(Tuple tuple) throws IOException{
		if(tuple == null|| tuple.size() == 0) {
		return false;
		}
			try {
				Object object= tuple.get(0);
					if(object == null){ 
						return false;}
				int i = (Integer) object;
					if(i == 18 || i == 19 || i == 21 || i == 23 || i == 27) {
						return true;
					} else {
						return false;
					}
		} catch (ExecException e){
			throw new IOException(e);
		}
	}
}
