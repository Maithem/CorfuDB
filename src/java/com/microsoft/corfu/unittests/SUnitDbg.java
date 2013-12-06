package com.microsoft.corfu.unittests;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuLogMark;
import com.microsoft.corfu.ExtntInfo;
import com.microsoft.corfu.ExtntWrap;

public class SUnitDbg {

	static CorfuConfigManager CM = null;
	static CorfuClientImpl crf = null;
	
	interface helper {
		void helperf(StringTokenizer I);
	}
	
	static helper printhelp = 
			new helper() {
			public void helperf(StringTokenizer I) {
				System.out.println("Usage: ");
				System.out.println("  help");
				System.out.println("  read <offset>");
				System.out.println("  meta <offset>");
				System.out.println("  bounds");
				System.out.println("  quit");
			}};
	
	static HashMap<String, helper> debugger = new HashMap<String, helper>(){

		{
			put("help", printhelp);
		
			put("meta", 
				new helper() {
				public void helperf(StringTokenizer I) {
					if (!I.hasMoreElements()) {
						printhelp.helperf(null);
						return;
					}
					String OffStr = I.nextToken();
					long off = Long.parseLong(OffStr);
					ExtntInfo inf;
					try {
						inf = crf.dbg(off);
						System.out.println("meta: " + inf);
					} catch (CorfuException e) {
						System.out.println("dbg failed off=" + off);
						e.printStackTrace();
					}
				}
			});
			
			put("read",
				new helper() {
				public void helperf(StringTokenizer I) {
					if (!I.hasMoreElements()) {
						printhelp.helperf(null);
						return;
					}
					String OffStr = I.nextToken();
					long off = Long.parseLong(OffStr);
					ExtntWrap ret;
					try {
						ret = crf.readExtnt(off);
						System.out.println("read: size=" + ret.getCtntSize() + " meta="+ ret.getInf());
					} catch (CorfuException e) {
						System.out.println("readExtnt failed off=" + off);
						e.printStackTrace();
					}
				}
			});
			
			put("bounds",
				new helper() {
				public void helperf(StringTokenizer I) {
					long head, tail, ctail;
					
					try {
						head = crf.checkLogMark(CorfuLogMark.HEAD); 
						tail = crf.checkLogMark(CorfuLogMark.TAIL); 
						ctail = crf.checkLogMark(CorfuLogMark.CONTIG); 
						System.out.println("Log boundaries (head, contiguous-tail, tail): (" + 
						head + ", " + ctail + ", " + tail + ")");
					} catch (CorfuException e) {
						System.out.println("checkLogMark failed");
					}
				}
			});
			
			put("quit", 
				new helper() {
				public void helperf(StringTokenizer I) { System.exit(0); }
			});
	}};
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		new Thread(new Runnable() {
			public void run() {

				BufferedReader c;

				try {
					CM = new CorfuConfigManager(new File("./0.aux"));
					crf = new CorfuClientImpl(CM);
		
					c = new BufferedReader(new InputStreamReader(System.in));
					for (;;) {
						System.out.print("> ");
						// System.out.flush();
						
						StringTokenizer I = new StringTokenizer(c.readLine());
						if (!I.hasMoreTokens()) continue;
						
						String cmd = I.nextToken();
						helper h = debugger.get(cmd);
						if (h == null) {
							h = debugger.get("help");
							if (h != null) h.helperf(null);
						} else {
							h.helperf(I);
						}
					}
		
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return ;
				}
			}
		}).run();
				
	}
}
