package com.test.weather;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.parser.AliasMasker.input_clause_return;

public class IfCorruptUDF extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
			return null;
		try{
			String str=(String) input.get(0);
			
			if(str.equals("-9999.0")){
				return "0";
			}else{
				return str;
			}
		}
		catch(Exception e){
			throw WrappedIOException.wrap("Caught exeception processing input row", e);
		}
	}

}
