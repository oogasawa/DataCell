package com.github.oogasawa.datacell.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.oogasawa.utility.files.FileIO;



public class DCTextComplementer {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");
	
	public static void main(String[] argv) {
		DCTextComplementer obj = new DCTextComplementer();
		obj.complement(argv[0]);
	}
	
	
	public void complement(String fpath) {
		Pattern pDs = Pattern.compile("^@ds\\s*:\\s*(\\S.+)", Pattern.CASE_INSENSITIVE);
		Pattern pId = Pattern.compile("^@id\\s*:\\s*(\\S.+)", Pattern.CASE_INSENSITIVE);
	
		Path  path = Paths.get(fpath);
		Path  fname = path.getFileName();
		
		
		BufferedReader br = null;
		String ds = null;
		String id = null;
		try {
			br = FileIO.getBufferedReader(fpath);
			String  line = null;
			while ((line = br.readLine()) != null) {
				Matcher m = pDs.matcher(line);
				if (m.find()) {
					ds = m.group(1);
					if (ds.equals("--")) {
						ds = fname.toString();
					}
					System.out.println("@ds: " + ds);
					continue;
				}
				m = pId.matcher(line);
				if (m.find()) {
					id = m.group(1);
					if (id.equals("--")) {
						id = UUID.randomUUID().toString();
					}
					System.out.println("@id: " + id);
					continue;
				}
				System.out.println(line);
			}
		} catch (FileNotFoundException e) {
			logger.throwing("com.github.oogasawa.datacell.util.DCTextComplementer", "complement", e);
		} catch (IOException e) {
			logger.throwing("com.github.oogasawa.datacell.util.DCTextComplementer", "complement", e);
		}
		finally {
			try {
				br.close();
			} catch (IOException e) {
				logger.throwing("com.github.oogasawa.datacell.util.DCTextComplementer", "complement", e);
			}
		}
	}
	

}
